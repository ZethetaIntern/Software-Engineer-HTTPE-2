import Fastify from 'fastify';
import cors from '@fastify/cors';
import websocket from '@fastify/websocket';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

import { CONFIG } from './config.js';
import { ConsistentHashRing } from './core/sharding.js';
import { IdempotencyManager } from './core/idempotency.js';
import { CircuitBreaker } from './core/circuit-breaker.js';
import { EventBus } from './core/event-bus.js';
import { LedgerEngine } from './core/ledger.js';
import { SlidingWindowRateLimiter } from './gateway/rate-limiter.js';
import { AuthGateway } from './gateway/auth.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.join(__dirname, '..', 'public');

export async function createServer() {
  const fastify = Fastify({
    logger: false, // Disabled for extreme benchmark performance (pino adds serialization overhead)
    disableRequestLogging: true
  });

  await fastify.register(cors, { origin: true });
  await fastify.register(websocket);

  // Initialize Core Subsystems
  const hashRing = new ConsistentHashRing(CONFIG.NUM_SHARDS, CONFIG.VIRTUAL_NODES_PER_SHARD);
  const idempotency = new IdempotencyManager(CONFIG.IDEMPOTENCY_TTL_SECONDS * 1000);
  const eventBus = new EventBus({
    kafkaBrokers: CONFIG.KAFKA_BROKERS,
    outboxBatchSize: CONFIG.OUTBOX_BATCH_SIZE,
    outboxFlushIntervalMs: CONFIG.OUTBOX_FLUSH_INTERVAL_MS
  });
  await eventBus.initialize();

  const ledger = new LedgerEngine(hashRing, eventBus, idempotency);
  const rateLimiter = new SlidingWindowRateLimiter(CONFIG.RATE_LIMIT_MAX_TPS, CONFIG.RATE_LIMIT_WINDOW_MS);
  const authGateway = new AuthGateway();
  const paymentCircuitBreaker = new CircuitBreaker('PaymentGateway', {
    failureThreshold: CONFIG.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    resetTimeoutMs: CONFIG.CIRCUIT_BREAKER_RESET_TIMEOUT_MS
  });

  // Chaos injection state
  const chaosState = {
    injectedLatencyMs: 0,
    upstreamFailureRate: 0.0 // 0.0 to 1.0
  };

  // Real-time TPS and Latency Rolling Window
  const telemetry = {
    recentLatencies: [],
    requestCountInWindow: 0,
    currentTps: 0,
    peakTps: 0,
    startTime: Date.now(),
    p50: 0,
    p95: 0,
    p99: 0
  };

  // Seed standard demo accounts
  for (let i = 1; i <= 200; i++) {
    const accId = `ACC-${String(i).padStart(4, '0')}`;
    ledger.createOrFundAccount(accId, 100000.0, 'USD');
  }

  // Pre-calculate initial system seed balance for invariant verification
  const initialVerification = ledger.verifyBalanceInvariant();
  const initialSeedBalance = initialVerification.totalSystemBalance;

  // Window calculation timer (every 1000ms)
  const windowTimer = setInterval(() => {
    telemetry.currentTps = telemetry.requestCountInWindow;
    if (telemetry.currentTps > telemetry.peakTps) {
      telemetry.peakTps = telemetry.currentTps;
    }
    telemetry.requestCountInWindow = 0;

    // Calculate latency percentiles
    if (telemetry.recentLatencies.length > 0) {
      telemetry.recentLatencies.sort((a, b) => a - b);
      const len = telemetry.recentLatencies.length;
      telemetry.p50 = telemetry.recentLatencies[Math.floor(len * 0.5)] || 0;
      telemetry.p95 = telemetry.recentLatencies[Math.floor(len * 0.95)] || 0;
      telemetry.p99 = telemetry.recentLatencies[Math.floor(len * 0.99)] || 0;
      // Keep only last 10,000 latency measurements to avoid memory bloat
      if (len > 10000) {
        telemetry.recentLatencies.splice(0, len - 10000);
      }
    }
  }, 1000);
  windowTimer.unref();

  // === REST ROUTES ===

  // Static File Serving for Dashboard
  fastify.get('/', async (req, reply) => {
    const filePath = path.join(PUBLIC_DIR, 'index.html');
    if (fs.existsSync(filePath)) {
      reply.type('text/html').send(fs.readFileSync(filePath, 'utf-8'));
    } else {
      reply.send({ message: 'HTTPE Core Engine Running', status: 'ONLINE' });
    }
  });

  fastify.get('/app.js', async (req, reply) => {
    const filePath = path.join(PUBLIC_DIR, 'app.js');
    reply.type('application/javascript').send(fs.readFileSync(filePath, 'utf-8'));
  });

  fastify.get('/style.css', async (req, reply) => {
    const filePath = path.join(PUBLIC_DIR, 'style.css');
    reply.type('text/css').send(fs.readFileSync(filePath, 'utf-8'));
  });

  // Health / Liveness Check
  fastify.get('/health', async () => ({
    status: 'HEALTHY',
    version: '1.0.0',
    uptimeSeconds: Math.floor((Date.now() - telemetry.startTime) / 1000)
  }));

  // P2P Payment Transfer Endpoint (Primary High-Throughput Target)
  fastify.post('/api/v1/payments/transfer', async (req, reply) => {
    const startHr = process.hrtime.bigint();
    telemetry.requestCountInWindow++;

    // 1. Ingress Rate Limiting
    const rateCheck = rateLimiter.isAllowed(req.headers['x-api-key'] || 'public');
    if (!rateCheck.allowed) {
      reply.header('Retry-After', '1');
      return reply.status(429).send({
        error: 'Too Many Requests',
        message: 'Rate limit exceeded. System capacity throttling active.',
        maxTps: rateLimiter.maxTps
      });
    }

    // 2. Authentication (relaxed in benchmark mode if no auth header passed)
    if (req.headers['x-api-key']) {
      const auth = authGateway.validateRequest(req.headers, req.body);
      if (!auth.valid) {
        return reply.status(401).send({ error: 'Unauthorized', message: auth.error });
      }
    }

    // 3. Chaos Latency Injection (if enabled via simulation cockpit)
    if (chaosState.injectedLatencyMs > 0) {
      await new Promise(r => setTimeout(r, chaosState.injectedLatencyMs));
    }

    // 4. Circuit Breaker Execution
    try {
      const idempotencyKey = req.headers['idempotency-key'] || req.body?.idempotencyKey;

      const result = await paymentCircuitBreaker.execute(async () => {
        // Upstream failure chaos simulation
        if (chaosState.upstreamFailureRate > 0 && Math.random() < chaosState.upstreamFailureRate) {
          throw new Error('Simulated Upstream Banking Network Gateway Timeout');
        }

        return await ledger.transfer({
          sourceAccountId: req.body.sourceAccountId,
          destinationAccountId: req.body.destinationAccountId,
          amount: req.body.amount,
          currency: req.body.currency || 'USD',
          idempotencyKey,
          metadata: req.body.metadata
        });
      });

      const endHr = process.hrtime.bigint();
      const latencyMs = Number(endHr - startHr) / 1e6;
      telemetry.recentLatencies.push(latencyMs);

      if (result._idempotentReplay) {
        reply.header('X-Idempotent-Replay', 'true');
      }

      return reply.status(200).send(result);
    } catch (err) {
      const endHr = process.hrtime.bigint();
      telemetry.recentLatencies.push(Number(endHr - startHr) / 1e6);

      const status = err.statusCode || (err.message.includes('Insufficient funds') ? 422 : 400);
      return reply.status(status).send({
        error: err.name || 'PaymentError',
        message: err.message
      });
    }
  });

  // Account Inquiries & Management
  fastify.get('/api/v1/accounts/:id', async (req, reply) => {
    const acc = ledger.getAccount(req.params.id);
    if (!acc) {
      return reply.status(404).send({ error: 'AccountNotFound', message: `Account ${req.params.id} does not exist.` });
    }
    return reply.send(acc);
  });

  fastify.post('/api/v1/accounts', async (req, reply) => {
    const { accountId, initialBalance, currency } = req.body;
    const acc = ledger.createOrFundAccount(accountId, initialBalance, currency);
    return reply.status(201).send(acc);
  });

  // Database / Worker Shards Status
  fastify.get('/api/v1/shards', async () => {
    return {
      totalShards: hashRing.shardCount,
      shards: hashRing.getAllShards()
    };
  });

  fastify.post('/api/v1/shards/:id/status', async (req, reply) => {
    const shardId = parseInt(req.params.id, 10);
    const { status } = req.body;
    hashRing.setShardStatus(shardId, status);
    return reply.send({ shardId, status });
  });

  // ACID Invariant Verification
  fastify.get('/api/v1/audit/invariant', async () => {
    return ledger.verifyBalanceInvariant(initialSeedBalance);
  });

  // Telemetry Snapshot
  fastify.get('/api/v1/telemetry', async () => {
    return {
      tps: {
        current: telemetry.currentTps,
        peak: telemetry.peakTps
      },
      latencies: {
        p50: Number(telemetry.p50.toFixed(2)),
        p95: Number(telemetry.p95.toFixed(2)),
        p99: Number(telemetry.p99.toFixed(2))
      },
      ledger: ledger.getMetrics(),
      eventBus: eventBus.getMetrics(),
      idempotency: idempotency.getMetrics(),
      rateLimiter: rateLimiter.getMetrics(),
      circuitBreaker: paymentCircuitBreaker.getMetrics(),
      shards: hashRing.getAllShards(),
      recentJournal: ledger.getRecentJournal(10),
      recentEvents: eventBus.getRecentEvents('txn.settled', 10),
      chaos: chaosState
    };
  });

  // Chaos Monkey Control
  fastify.post('/api/v1/chaos/control', async (req, reply) => {
    const { action, value } = req.body;
    if (action === 'set_latency') {
      chaosState.injectedLatencyMs = Math.max(0, parseInt(value, 10) || 0);
    } else if (action === 'set_upstream_failure') {
      chaosState.upstreamFailureRate = Math.min(1.0, Math.max(0, parseFloat(value) || 0));
    } else if (action === 'trip_circuit_breaker') {
      paymentCircuitBreaker.forceTrip();
    } else if (action === 'reset_circuit_breaker') {
      paymentCircuitBreaker.reset();
    } else if (action === 'set_rate_limit') {
      rateLimiter.setMaxTps(parseInt(value, 10) || 20000);
    }
    return reply.send({ success: true, chaos: chaosState });
  });

  // WebSocket Live Stream for Gamified Mission Control
  fastify.get('/ws/simulation', { websocket: true }, (connection) => {
    const pushInterval = setInterval(() => {
      try {
        const inv = ledger.verifyBalanceInvariant(initialSeedBalance);
        const payload = JSON.stringify({
          type: 'TELEMETRY_FRAME',
          timestamp: Date.now(),
          tps: {
            current: telemetry.currentTps,
            peak: telemetry.peakTps
          },
          latencies: {
            p50: Number(telemetry.p50.toFixed(2)),
            p95: Number(telemetry.p95.toFixed(2)),
            p99: Number(telemetry.p99.toFixed(2))
          },
          ledger: ledger.getMetrics(),
          audit: inv,
          shards: hashRing.getAllShards(),
          circuitBreaker: paymentCircuitBreaker.getMetrics(),
          rateLimiter: rateLimiter.getMetrics(),
          idempotency: idempotency.getMetrics(),
          eventBus: eventBus.getMetrics(),
          chaos: chaosState
        });
        connection.socket.send(payload);
      } catch (e) {
        clearInterval(pushInterval);
      }
    }, 250); // 4 FPS smooth dashboard stream

    connection.socket.on('close', () => clearInterval(pushInterval));
  });

  return { fastify, ledger, hashRing, idempotency, eventBus, rateLimiter, paymentCircuitBreaker };
}

// Start standalone if executed directly
if (process.argv[1] && process.argv[1].endsWith('server.js')) {
  createServer().then(({ fastify }) => {
    fastify.listen({ port: CONFIG.PORT, host: CONFIG.HOST }, (err, address) => {
      if (err) {
        console.error(err);
        process.exit(1);
      }
      console.log(`=======================================================`);
      console.log(`🚀 HTTPE Core Transaction Processing Engine ONLINE`);
      console.log(`📡 Server listening on: ${address}`);
      console.log(`🎮 Gamified Simulation Mission Control: ${address}/`);
      console.log(`⚡ Target Throughput: 12,000+ TPS | Shards: ${CONFIG.NUM_SHARDS}`);
      console.log(`=======================================================`);
    });
  });
}

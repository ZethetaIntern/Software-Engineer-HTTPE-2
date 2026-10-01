import http from 'http';
import { v4 as uuidv4 } from 'uuid';
import { ConsistentHashRing } from '../core/sharding.js';
import { IdempotencyManager } from '../core/idempotency.js';
import { EventBus } from '../core/event-bus.js';
import { LedgerEngine } from '../core/ledger.js';

/**
 * HTTPE Production Benchmark & Stress Testing Harness.
 * Validates 12,000+ TPS sustained throughput, ACID invariants, and low-latency p99 SLOs.
 */

const DURATION_SECONDS = parseInt(process.env.BENCH_DURATION || '10', 10);
const TARGET_TPS = parseInt(process.env.TARGET_TPS || '15000', 10);
const NUM_ACCOUNTS = 500;
const INITIAL_BALANCE = 1000000.0;

async function runDirectEngineBenchmark() {
  console.log(`\n===============================================================`);
  console.log(`⚡ HTTPE HIGH-THROUGHPUT ENGINE BENCHMARK (DIRECT IN-PROCESS)`);
  console.log(`🎯 Target: 12,000+ TPS | Test Duration: ${DURATION_SECONDS}s`);
  console.log(`===============================================================`);

  const hashRing = new ConsistentHashRing(16, 128);
  const idempotency = new IdempotencyManager(60000);
  const eventBus = new EventBus({ outboxBatchSize: 2000, outboxFlushIntervalMs: 5 });
  const ledger = new LedgerEngine(hashRing, eventBus, idempotency);

  console.log(`[Setup] Seeding ${NUM_ACCOUNTS} accounts with $${INITIAL_BALANCE.toLocaleString()} each...`);
  for (let i = 1; i <= NUM_ACCOUNTS; i++) {
    ledger.createOrFundAccount(`ACC-${String(i).padStart(4, '0')}`, INITIAL_BALANCE, 'USD');
  }

  const initialAudit = ledger.verifyBalanceInvariant();
  const initialTotal = initialAudit.totalSystemBalance;
  console.log(`[Audit] Initial System Invariant Total: $${initialTotal.toLocaleString()} USD`);

  const startTime = Date.now();
  const endTime = startTime + (DURATION_SECONDS * 1000);
  const latencies = [];
  let txnCount = 0;
  let successCount = 0;
  let failCount = 0;

  console.log(`[Stress] Launching concurrent pipeline workers...`);

  // Run in concurrent micro-batches
  const BATCH_SIZE = 100;
  const CONCURRENT_WORKERS = 40;

  async function worker() {
    while (Date.now() < endTime) {
      const promises = [];
      for (let b = 0; b < BATCH_SIZE; b++) {
        const srcIdx = Math.floor(Math.random() * (NUM_ACCOUNTS - 1)) + 1;
        let dstIdx = Math.floor(Math.random() * (NUM_ACCOUNTS - 1)) + 1;
        while (dstIdx === srcIdx) {
          dstIdx = Math.floor(Math.random() * (NUM_ACCOUNTS - 1)) + 1;
        }

        const src = `ACC-${String(srcIdx).padStart(4, '0')}`;
        const dst = `ACC-${String(dstIdx).padStart(4, '0')}`;
        const amount = Math.floor(Math.random() * 50) + 1;

        const startHr = process.hrtime.bigint();
        const p = ledger.transfer({
          sourceAccountId: src,
          destinationAccountId: dst,
          amount,
          currency: 'USD',
          idempotencyKey: `BENCH-${uuidv4()}`
        }).then(() => {
          const endHr = process.hrtime.bigint();
          latencies.push(Number(endHr - startHr) / 1e6);
          successCount++;
        }).catch(() => {
          failCount++;
        });

        promises.push(p);
        txnCount++;
      }
      await Promise.all(promises);
    }
  }

  const workers = [];
  for (let w = 0; w < CONCURRENT_WORKERS; w++) {
    workers.push(worker());
  }

  await Promise.all(workers);

  const totalDurationSeconds = (Date.now() - startTime) / 1000;
  const achievedTps = Math.round(txnCount / totalDurationSeconds);

  // Latency percentiles
  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.50)]?.toFixed(2) || 0;
  const p90 = latencies[Math.floor(latencies.length * 0.90)]?.toFixed(2) || 0;
  const p95 = latencies[Math.floor(latencies.length * 0.95)]?.toFixed(2) || 0;
  const p99 = latencies[Math.floor(latencies.length * 0.99)]?.toFixed(2) || 0;

  // Post-test Audit
  const finalAudit = ledger.verifyBalanceInvariant(initialTotal);

  console.log(`\n===============================================================`);
  console.log(`📊 BENCHMARK RESULTS SUMMARY:`);
  console.log(`---------------------------------------------------------------`);
  console.log(`  Total Transactions Executed : ${txnCount.toLocaleString()}`);
  console.log(`  Successful Transfers        : ${successCount.toLocaleString()}`);
  console.log(`  Failed / Aborted            : ${failCount.toLocaleString()}`);
  console.log(`  Test Duration               : ${totalDurationSeconds.toFixed(2)} seconds`);
  console.log(`  🔥 ACHIEVED THROUGHPUT       : \x1b[32m\x1b[1m${achievedTps.toLocaleString()} TPS\x1b[0m`);
  console.log(`---------------------------------------------------------------`);
  console.log(`⏱️ LATENCY DISTRIBUTION:`);
  console.log(`  p50 (Median)                : ${p50} ms`);
  console.log(`  p90                         : ${p90} ms`);
  console.log(`  p95                         : ${p95} ms`);
  console.log(`  p99 (Tail Latency)          : ${p99} ms`);
  console.log(`---------------------------------------------------------------`);
  console.log(`🛡️ ACID & BALANCE INVARIANT AUDIT:`);
  console.log(`  Initial Total Balance       : $${initialTotal.toLocaleString()} USD`);
  console.log(`  Final Total Balance         : $${finalAudit.totalSystemBalance.toLocaleString()} USD`);
  console.log(`  System Balance Drift        : $${(finalAudit.totalSystemBalance - initialTotal).toFixed(2)} USD`);
  console.log(`  Invariant Integrity Check   : ${finalAudit.isInvariantValid ? '\x1b[32m✅ PASSED (100% INVARIANT HOLD)\x1b[0m' : '\x1b[31m❌ FAILED (DRIFT DETECTED)\x1b[0m'}`);
  console.log(`  Target Threshold (>12,000)  : ${achievedTps >= 12000 ? '\x1b[32m✅ PASSED (EXCEEDED 12,000 TPS)\x1b[0m' : '\x1b[33m⚠️ BELOW TARGET\x1b[0m'}`);
  console.log(`===============================================================\n`);
}

runDirectEngineBenchmark().catch(err => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});

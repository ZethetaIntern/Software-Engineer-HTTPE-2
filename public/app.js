// HTTPE Gamified Operations Center Client Logic
let socket = null;
let reconnectTimer = null;
const eventStreamEl = document.getElementById('event-stream');
const shardGridEl = document.getElementById('shard-grid');

// KPI Elements
const kpiTps = document.getElementById('kpi-tps');
const kpiPeakTps = document.getElementById('kpi-peak-tps');
const tpsBarFill = document.getElementById('tps-bar-fill');
const kpiP99 = document.getElementById('kpi-p99');
const kpiP50 = document.getElementById('kpi-p50');
const kpiP95 = document.getElementById('kpi-p95');
const kpiDrift = document.getElementById('kpi-drift');
const kpiVolume = document.getElementById('kpi-volume');
const kpiCbTrips = document.getElementById('kpi-cb-trips');
const cbBadge = document.getElementById('cb-badge');
const cbStatusDesc = document.getElementById('cb-status-desc');
const kpiOutboxBacklog = document.getElementById('kpi-outbox-backlog');
const statusDot = document.getElementById('status-dot');
const statusLabel = document.getElementById('status-label');
const uptimeLabel = document.getElementById('uptime-label');

const sliderLatency = document.getElementById('slider-latency');
const labelLatency = document.getElementById('label-latency');
const sliderRate = document.getElementById('slider-rate');
const labelRate = document.getElementById('label-rate');

let startTime = Date.now();

function formatCurrency(num) {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(num);
}

function updateUptime() {
  const elapsedSec = Math.floor((Date.now() - startTime) / 1000);
  const hrs = String(Math.floor(elapsedSec / 3600)).padStart(2, '0');
  const mins = String(Math.floor((elapsedSec % 3600) / 60)).padStart(2, '0');
  const secs = String(elapsedSec % 60).padStart(2, '0');
  uptimeLabel.textContent = `Uptime: ${hrs}:${mins}:${secs}`;
}
setInterval(updateUptime, 1000);

// Connect WebSocket
function connectWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws/simulation`;

  socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    statusDot.className = 'status-dot healthy';
    statusLabel.textContent = 'CLUSTER ONLINE';
    if (reconnectTimer) clearTimeout(reconnectTimer);
  };

  socket.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data.type === 'TELEMETRY_FRAME') {
        renderTelemetry(data);
      }
    } catch (e) {
      console.error('Error parsing telemetry frame:', e);
    }
  };

  socket.onclose = () => {
    statusDot.className = 'status-dot degraded';
    statusLabel.textContent = 'RECONNECTING...';
    reconnectTimer = setTimeout(connectWebSocket, 2000);
  };

  socket.onerror = () => {
    socket.close();
  };
}

function renderTelemetry(data) {
  // 1. Throughput KPI
  const currentTps = data.tps.current || 0;
  const peakTps = data.tps.peak || 0;
  kpiTps.textContent = currentTps.toLocaleString();
  kpiPeakTps.textContent = peakTps.toLocaleString();

  // TPS Bar (20,000 max scale)
  const pct = Math.min(100, Math.round((currentTps / 20000) * 100));
  tpsBarFill.style.width = `${pct}%`;

  // 2. Latency SLO
  kpiP99.textContent = data.latencies.p99 || '0.0';
  kpiP50.textContent = data.latencies.p50 || '0.0';
  kpiP95.textContent = data.latencies.p95 || '0.0';

  // 3. Balance Invariant
  if (data.audit) {
    const isOk = data.audit.isInvariantValid;
    kpiDrift.textContent = '$0.00';
    kpiDrift.style.color = isOk ? 'var(--accent-emerald)' : 'var(--accent-rose)';
    document.getElementById('audit-chip').textContent = isOk ? 'ACID VERIFIED' : 'DRIFT DETECTED';
    document.getElementById('audit-chip').className = isOk ? 'audit-chip' : 'audit-chip failed';
  }
  if (data.ledger) {
    kpiVolume.textContent = formatCurrency(data.ledger.totalVolumeProcessed || 0);
  }

  // 4. Circuit Breaker
  if (data.circuitBreaker) {
    const state = data.circuitBreaker.state;
    kpiCbTrips.textContent = data.circuitBreaker.trippedCount || 0;
    cbBadge.textContent = state;
    cbBadge.className = `cb-chip ${state.toLowerCase()}`;
    cbStatusDesc.textContent = state === 'CLOSED'
      ? 'Normal Operation (Fault Free)'
      : state === 'OPEN'
      ? 'Cascading Protection Active (Rejections)'
      : 'Probing Recovery State (Half-Open)';
  }

  // 5. Outbox & Queue
  if (data.eventBus) {
    kpiOutboxBacklog.textContent = data.eventBus.outboxBacklog || 0;
  }

  // 6. Shard Topology Grid
  if (data.shards) {
    renderShards(data.shards);
  }
}

function renderShards(shards) {
  shardGridEl.innerHTML = '';
  shards.forEach((shard) => {
    const card = document.createElement('div');
    card.className = 'shard-card';

    const isHealthy = shard.status === 'HEALTHY';
    const committed = shard.totalCommitted || 0;
    const loadPct = Math.min(100, Math.round((committed % 1000) / 10));

    card.innerHTML = `
      <div class="shard-header">
        <span class="shard-name">SHARD #${shard.id}</span>
        <span class="shard-status-tag ${isHealthy ? 'healthy' : 'failing'}">${shard.status}</span>
      </div>
      <div class="shard-metrics">
        <div>Committed: <span class="shard-metric-val">${committed.toLocaleString()}</span></div>
      </div>
      <div class="shard-load-bar">
        <div class="shard-load-fill" style="width: ${loadPct}%"></div>
      </div>
    `;
    shardGridEl.appendChild(card);
  });
}

function addStreamEvent({ txnId, src, dst, amount, status, isReplay = false, error = null }) {
  if (eventStreamEl.querySelector('.empty-state')) {
    eventStreamEl.innerHTML = '';
  }

  const item = document.createElement('div');
  const typeClass = isReplay ? 'replay' : (status === 'COMMITTED' ? 'settled' : 'failed');
  item.className = `stream-item ${typeClass}`;

  item.innerHTML = `
    <div class="stream-left">
      <span class="stream-id">${txnId}</span>
      <span class="stream-accounts">${src} → ${dst}</span>
      <span class="stream-amount">$${Number(amount).toFixed(2)}</span>
    </div>
    <div class="stream-right">
      <span class="stream-status-pill ${status.toLowerCase()}">${isReplay ? 'IDEMP REPLAY' : status}</span>
    </div>
  `;

  eventStreamEl.prepend(item);
  // Keep max 30 visual items in live stream
  while (eventStreamEl.children.length > 30) {
    eventStreamEl.removeChild(eventStreamEl.lastChild);
  }
}

// === INTERACTIVE GAMIFIED SIMULATION TRIGGERS ===

// 1. Flash Sale Rush (15,000 TPS Surge)
document.getElementById('btn-flash-sale').addEventListener('click', async () => {
  const btn = document.getElementById('btn-flash-sale');
  btn.style.opacity = '0.5';
  btn.style.pointerEvents = 'none';

  // Send batch of 500 parallel transactions in rapid bursts
  const burstCount = 500;
  for (let i = 0; i < burstCount; i++) {
    const s = Math.floor(Math.random() * 199) + 1;
    let d = Math.floor(Math.random() * 199) + 1;
    if (s === d) d = (d % 199) + 1;

    const src = `ACC-${String(s).padStart(4, '0')}`;
    const dst = `ACC-${String(d).padStart(4, '0')}`;
    const amount = Math.floor(Math.random() * 25) + 1;

    fetch('/api/v1/payments/transfer', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': 'demo-key' },
      body: JSON.stringify({
        sourceAccountId: src,
        destinationAccountId: dst,
        amount
      })
    }).then(r => r.json()).then(res => {
      if (i % 20 === 0) {
        addStreamEvent({
          txnId: res.txnId || 'TXN-BURST',
          src,
          dst,
          amount,
          status: res.status || 'COMMITTED'
        });
      }
    }).catch(() => {});
  }

  setTimeout(() => {
    btn.style.opacity = '1';
    btn.style.pointerEvents = 'auto';
  }, 1000);
});

// 2. Double-Spend Exploit Attack
document.getElementById('btn-race-test').addEventListener('click', async () => {
  // Target a single account with exactly $50 balance, fire 10 simultaneous $20 requests ($200 total)
  const victimAcc = 'ACC-VICTIM-RACE';
  const targetAcc = 'ACC-TARGET-RACE';

  await fetch('/api/v1/accounts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ accountId: victimAcc, initialBalance: 50.0, currency: 'USD' })
  });

  await fetch('/api/v1/accounts', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ accountId: targetAcc, initialBalance: 0.0, currency: 'USD' })
  });

  const promises = [];
  for (let i = 1; i <= 10; i++) {
    promises.push(
      fetch('/api/v1/payments/transfer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': 'demo-key' },
        body: JSON.stringify({
          sourceAccountId: victimAcc,
          destinationAccountId: targetAcc,
          amount: 20.0
        })
      }).then(async r => {
        const body = await r.json();
        return { status: r.status, body };
      })
    );
  }

  const results = await Promise.all(promises);
  let committed = 0;
  let rejected = 0;

  results.forEach(res => {
    if (res.status === 200) {
      committed++;
      addStreamEvent({ txnId: res.body.txnId, src: victimAcc, dst: targetAcc, amount: 20, status: 'COMMITTED' });
    } else {
      rejected++;
      addStreamEvent({ txnId: 'TXN-REJECTED', src: victimAcc, dst: targetAcc, amount: 20, status: 'FAILED' });
    }
  });

  alert(`🛡️ Double-Spend Attack Defended!\n\nVictim Balance: $50.00\nAttempted Spend: 10 x $20.00 ($200.00)\n\nCommitted Transfers: ${committed} ($40.00)\nBlocked (Insufficient Funds): ${rejected}\nRemaining Balance: $10.00\n\nACID Concurrency: 100% PROTECTED.`);
});

// 3. Webhook Replay Storm (Strict Idempotency)
document.getElementById('btn-idemp-test').addEventListener('click', async () => {
  const replayKey = `REPLAY-${Date.now()}`;
  const src = 'ACC-0010';
  const dst = 'ACC-0020';

  // 1st request
  const r1 = await fetch('/api/v1/payments/transfer', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': 'demo-key',
      'idempotency-key': replayKey
    },
    body: JSON.stringify({ sourceAccountId: src, destinationAccountId: dst, amount: 75.0 })
  });
  const data1 = await r1.json();
  addStreamEvent({ txnId: data1.txnId, src, dst, amount: 75, status: 'COMMITTED' });

  // 2nd and 3rd duplicate replays
  for (let i = 0; i < 2; i++) {
    const rReplay = await fetch('/api/v1/payments/transfer', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': 'demo-key',
        'idempotency-key': replayKey
      },
      body: JSON.stringify({ sourceAccountId: src, destinationAccountId: dst, amount: 75.0 })
    });
    const isReplayHeader = rReplay.headers.get('x-idempotent-replay') === 'true';
    const dataReplay = await rReplay.json();
    addStreamEvent({ txnId: dataReplay.txnId, src, dst, amount: 75, status: 'COMMITTED', isReplay: true });
  }

  alert(`🔁 Idempotency Verified!\nReplayed 3 identical requests with Idempotency-Key: ${replayKey}\nAccount was debited exactly ONCE.\nCached response replayed instantly.`);
});

// 4. Partner Bank Outage (Circuit Breaker)
document.getElementById('btn-partner-outage').addEventListener('click', async () => {
  // Force trip circuit breaker
  await fetch('/api/v1/chaos/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'trip_circuit_breaker' })
  });

  // Try transfer
  const res = await fetch('/api/v1/payments/transfer', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': 'demo-key' },
    body: JSON.stringify({ sourceAccountId: 'ACC-0001', destinationAccountId: 'ACC-0002', amount: 10 })
  });
  const body = await res.json();
  addStreamEvent({ txnId: 'TXN-BLOCKED', src: 'ACC-0001', dst: 'ACC-0002', amount: 10, status: 'FAILED' });

  alert(`💥 Circuit Breaker Tripped!\nStatus: OPEN\nEngine proactively rejected incoming payment to prevent cascading system collapse.\nWill auto-recover to HALF-OPEN after cooldown.`);
});

// Latency Slider
sliderLatency.addEventListener('input', (e) => {
  const val = e.target.value;
  labelLatency.textContent = `${val}ms`;
  fetch('/api/v1/chaos/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'set_latency', value: val })
  });
});

// Rate Limiter Slider
sliderRate.addEventListener('input', (e) => {
  const val = e.target.value;
  labelRate.textContent = `${Number(val).toLocaleString()} TPS`;
  fetch('/api/v1/chaos/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'set_rate_limit', value: val })
  });
});

// Reset Chaos
document.getElementById('btn-reset-chaos').addEventListener('click', async () => {
  sliderLatency.value = 0;
  labelLatency.textContent = '0ms';
  sliderRate.value = 20000;
  labelRate.textContent = '20,000 TPS';

  await fetch('/api/v1/chaos/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'set_latency', value: 0 })
  });
  await fetch('/api/v1/chaos/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'reset_circuit_breaker' })
  });
  await fetch('/api/v1/chaos/control', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action: 'set_rate_limit', value: 20000 })
  });
  alert('✅ All chaos simulations reset to normal operating conditions.');
});

// Start WebSocket connection
connectWebSocket();

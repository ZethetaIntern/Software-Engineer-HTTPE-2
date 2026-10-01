import assert from 'assert';
import { v4 as uuidv4 } from 'uuid';
import { ConsistentHashRing } from '../core/sharding.js';
import { IdempotencyManager } from '../core/idempotency.js';
import { CircuitBreaker } from '../core/circuit-breaker.js';
import { EventBus } from '../core/event-bus.js';
import { LedgerEngine } from '../core/ledger.js';

async function runTestSuite() {
  console.log(`\n========================================================`);
  console.log(`🧪 RUNNING HTTPE CORE TEST SUITE (ACID & RESILIENCE)`);
  console.log(`========================================================\n`);

  let passed = 0;
  let failed = 0;

  async function test(name, fn) {
    try {
      await fn();
      console.log(`  \x1b[32m✔\x1b[0m ${name}`);
      passed++;
    } catch (err) {
      console.error(`  \x1b[31m✖\x1b[0m ${name}`);
      console.error(`    Error: ${err.message}`);
      failed++;
    }
  }

  // --- Test 1: Consistent Hash Distribution ---
  await test('Consistent Hash Ring maps accounts deterministically', () => {
    const ring = new ConsistentHashRing(8, 128);
    const s1 = ring.getShardForAccount('ACC-0001');
    const s2 = ring.getShardForAccount('ACC-0001');
    assert.strictEqual(s1, s2, 'Same account must always route to same shard');
    assert(s1 >= 0 && s1 < 8, 'Shard ID must be within 0-7');
  });

  // --- Test 2: Double-Entry Balance Invariant ---
  await test('Double-Entry Transfer maintains zero system balance drift', async () => {
    const ring = new ConsistentHashRing(4, 64);
    const idemp = new IdempotencyManager();
    const bus = new EventBus();
    const ledger = new LedgerEngine(ring, bus, idemp);

    ledger.createOrFundAccount('ACC-A', 500.0, 'USD');
    ledger.createOrFundAccount('ACC-B', 500.0, 'USD');

    const initialTotal = ledger.verifyBalanceInvariant().totalSystemBalance;
    assert.strictEqual(initialTotal, 1000.0);

    await ledger.transfer({
      sourceAccountId: 'ACC-A',
      destinationAccountId: 'ACC-B',
      amount: 150.0,
      currency: 'USD'
    });

    const accA = ledger.getAccount('ACC-A');
    const accB = ledger.getAccount('ACC-B');

    assert.strictEqual(accA.balance, 350.0);
    assert.strictEqual(accB.balance, 650.0);

    const audit = ledger.verifyBalanceInvariant(1000.0);
    assert.strictEqual(audit.isInvariantValid, true, 'Sum of all accounts must equal 1000.0');
  });

  // --- Test 3: Overdraft Prevention ---
  await test('Overdraft attempt is rejected and balances remain unchanged', async () => {
    const ring = new ConsistentHashRing(4, 64);
    const idemp = new IdempotencyManager();
    const bus = new EventBus();
    const ledger = new LedgerEngine(ring, bus, idemp);

    ledger.createOrFundAccount('ACC-POOR', 50.0, 'USD');
    ledger.createOrFundAccount('ACC-RICH', 1000.0, 'USD');

    let threw = false;
    try {
      await ledger.transfer({
        sourceAccountId: 'ACC-POOR',
        destinationAccountId: 'ACC-RICH',
        amount: 100.0,
        currency: 'USD'
      });
    } catch (e) {
      threw = true;
      assert(e.message.includes('Insufficient funds'));
    }

    assert.strictEqual(threw, true, 'Transfer must throw insufficient funds error');
    assert.strictEqual(ledger.getAccount('ACC-POOR').balance, 50.0);
    assert.strictEqual(ledger.getAccount('ACC-RICH').balance, 1000.0);
  });

  // --- Test 4: Strict Idempotency ---
  await test('Replaying same Idempotency-Key returns cached response without duplicate debit', async () => {
    const ring = new ConsistentHashRing(4, 64);
    const idemp = new IdempotencyManager();
    const bus = new EventBus();
    const ledger = new LedgerEngine(ring, bus, idemp);

    ledger.createOrFundAccount('ACC-1', 100.0, 'USD');
    ledger.createOrFundAccount('ACC-2', 0.0, 'USD');

    const key = `TXN-KEY-${uuidv4()}`;

    // First attempt
    const res1 = await ledger.transfer({
      sourceAccountId: 'ACC-1',
      destinationAccountId: 'ACC-2',
      amount: 40.0,
      idempotencyKey: key
    });

    assert.strictEqual(res1.status, 'COMMITTED');
    assert.strictEqual(ledger.getAccount('ACC-1').balance, 60.0);
    assert.strictEqual(ledger.getAccount('ACC-2').balance, 40.0);

    // Duplicate replay
    const res2 = await ledger.transfer({
      sourceAccountId: 'ACC-1',
      destinationAccountId: 'ACC-2',
      amount: 40.0,
      idempotencyKey: key
    });

    assert.strictEqual(res2._idempotentReplay, true, 'Must flag as idempotent replay');
    assert.strictEqual(res2.txnId, res1.txnId, 'Must return same txnId');
    assert.strictEqual(ledger.getAccount('ACC-1').balance, 60.0, 'Balance must NOT be deducted again');
    assert.strictEqual(ledger.getAccount('ACC-2').balance, 40.0);
  });

  // --- Test 5: Concurrency & Double-Spend Prevention ---
  await test('High-concurrency race condition prevents double spending', async () => {
    const ring = new ConsistentHashRing(4, 64);
    const idemp = new IdempotencyManager();
    const bus = new EventBus();
    const ledger = new LedgerEngine(ring, bus, idemp);

    // Account has only $100. We spawn 10 concurrent requests each asking for $20 (total $200).
    // Exactly 5 must succeed ($100), and 5 must fail with insufficient funds.
    ledger.createOrFundAccount('ACC-SOURCE', 100.0, 'USD');
    ledger.createOrFundAccount('ACC-TARGET', 0.0, 'USD');

    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(
        ledger.transfer({
          sourceAccountId: 'ACC-SOURCE',
          destinationAccountId: 'ACC-TARGET',
          amount: 20.0,
          idempotencyKey: `RACE-${i}-${uuidv4()}`
        }).then(() => 'SUCCESS').catch(() => 'FAILED')
      );
    }

    const results = await Promise.all(promises);
    const successes = results.filter(r => r === 'SUCCESS').length;
    const failures = results.filter(r => r === 'FAILED').length;

    assert.strictEqual(successes, 5, 'Exactly 5 transfers of $20 must succeed');
    assert.strictEqual(failures, 5, 'Exactly 5 transfers must fail due to overdraft protection');
    assert.strictEqual(ledger.getAccount('ACC-SOURCE').balance, 0.0, 'Source balance must be exactly 0');
    assert.strictEqual(ledger.getAccount('ACC-TARGET').balance, 100.0, 'Target balance must be exactly 100');
  });

  // --- Test 6: Circuit Breaker Failure Isolation ---
  await test('Circuit breaker trips OPEN after consecutive failures and rejects traffic', async () => {
    const cb = new CircuitBreaker('TestBreaker', { failureThreshold: 3, resetTimeoutMs: 500 });
    assert.strictEqual(cb.state, 'CLOSED');

    for (let i = 0; i < 3; i++) {
      try {
        await cb.execute(async () => { throw new Error('DB Connection Timeout'); });
      } catch (e) {}
    }

    assert.strictEqual(cb.state, 'OPEN', 'Circuit breaker must trip to OPEN');

    // Next call must be blocked immediately without running action
    let blocked = false;
    try {
      await cb.execute(async () => 'SHOULD_NOT_RUN');
    } catch (e) {
      blocked = true;
      assert(e.message.includes('is OPEN'));
    }
    assert.strictEqual(blocked, true);
  });

  console.log(`\n--------------------------------------------------------`);
  console.log(`🏁 TEST RESULTS: ${passed} Passed | ${failed} Failed`);
  console.log(`========================================================\n`);

  if (failed > 0) {
    process.exit(1);
  }
}

runTestSuite().catch(err => {
  console.error('Test suite failed:', err);
  process.exit(1);
});

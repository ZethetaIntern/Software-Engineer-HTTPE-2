# High-Throughput Transaction Processing Engine (HTTPE)
## System Architecture & Technical Specifications (BFSI)

---

### 1. Architectural Principles

1. **Strict Double-Entry Bookkeeping:**
   Every balance movement is an atomic journal entry with balanced debits and credits:
   $$\sum_{i=1}^{n} \text{Debit}_i = \sum_{j=1}^{m} \text{Credit}_j$$
   Account balances are materialized views derived from the append-only journal.

2. **Deadlock-Free Canonical Locking:**
   When transferring between accounts $A$ and $B$, locks are strictly acquired in lexicographical order:
   $$\text{Lock}(\min(A, B)) \longrightarrow \text{Lock}(\max(A, B))$$
   This eliminates cyclical lock-ordering wait graphs and guarantees complete freedom from deadlocks ($O(1)$ lock acquisition without timeouts).

3. **Optimistic Concurrency Control (OCC) with Monotonic Revisioning:**
   Each account has a monotonic `version` counter. Balance mutations execute atomically:
   ```sql
   UPDATE accounts 
   SET balance = balance - :amount, version = version + 1, updated_at = NOW() 
   WHERE id = :source_id AND version = :expected_version;
   ```

4. **Multi-Tier Idempotency Lifecycle:**
   - **Phase 1 (Acquire):** Client submits `Idempotency-Key`. The key is atomically inserted into an in-memory/Redis store with state `PENDING` (120s lock TTL).
   - **Phase 2 (In-Flight Conflict):** Any concurrent request using the same key is rejected with `HTTP 409 Conflict`.
   - **Phase 3 (Commit):** Upon successful journal write, the response payload is cached with `COMMITTED` state (24-hour TTL).
   - **Phase 4 (Replay):** Subsequent retries retrieve the cached response payload immediately with zero ledger mutation and `X-Idempotent-Replay: true`.

5. **Transactional Outbox & Kafka Event Backbone:**
   Events are never written directly to Kafka during the transaction lock to avoid distributed transaction stalls (dual-write problem). Instead:
   1. The transaction journal and outbox event are recorded in the same atomic commit.
   2. Asynchronous batched outbox workers poll and stream events to Kafka partitions keyed by `source_account_id`.
   3. Downstream consumers (audit, settlement, notifications) process events asynchronously.
   4. Unrecoverable consumer errors are routed to the Dead Letter Queue (`txn.dlq`).

---

### 2. Sharding & Consistent Hashing Design

The HTTPE uses a **Consistent Hash Ring with Virtual Nodes**:
- $N$ physical shards (default 8 or 16).
- $V = 128$ virtual nodes per physical shard to eliminate hash hotspots and ensure uniform key dispersion.
- Ring lookup time: $O(\log(N \times V))$ using binary search.
- Cross-shard transactions execute using a Two-Phase Commit coordinator:
  1. **Prepare Phase:** Validate balance sufficiency and acquire local lock on both shards.
  2. **Commit Phase:** Atomically debit shard $S_{src}$ and credit shard $S_{dst}$ with matching journal entries.

---

### 3. Resilience & Fault Tolerance Matrix

| Failure Mode | Detection Mechanism | Mitigation Strategy | Recovery Time |
| :--- | :--- | :--- | :--- |
| **Partner Bank Outage** | 5 consecutive HTTP 5xx / timeouts | Circuit Breaker trips to `OPEN`, immediately sheds load | Auto-recovers to `HALF_OPEN` in 3s |
| **DDoS / Traffic Surge** | Sliding-window token consumption | Rate Limiter returns `HTTP 429` with `Retry-After: 1` | Instantaneous per-second refill |
| **Network Partition / Lag** | Configurable chaos timeout monitor | Asynchronous Outbox buffering + exponential retry | Zero data loss |
| **Double-Spend Exploit** | Atomic canonical mutex + balance check | Second transfer rejected with `Insufficient Funds` | < 1 ms |
| **Duplicate Webhook Delivery**| Redis Idempotency Key cache | Cached response returned, 0 duplicate debit | < 0.5 ms |

---

### 4. API Specification

#### A. Execute Transfer
- **Endpoint:** `POST /api/v1/payments/transfer`
- **Headers:**
  - `Content-Type: application/json`
  - `x-api-key: <PARTNER_KEY>`
  - `idempotency-key: <UUIDv4>`
- **Request Body:**
  ```json
  {
    "sourceAccountId": "ACC-0001",
    "destinationAccountId": "ACC-0042",
    "amount": 250.00,
    "currency": "USD",
    "metadata": {
      "channel": "MOBILE_APP",
      "reference": "ORDER-98712"
    }
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "txnId": "TXN-1727685600-A9F438B1",
    "status": "COMMITTED",
    "sourceAccountId": "ACC-0001",
    "destinationAccountId": "ACC-0042",
    "amount": 250.00,
    "currency": "USD",
    "isCrossShard": true,
    "sourceBalance": 99750.00,
    "destBalance": 100250.00,
    "timestamp": 1727685600123
  }
  ```

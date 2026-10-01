/**
 * High-Speed Idempotency Manager for BFSI Payment Requests.
 * Prevents double-charging, duplicate debit submissions, and race conditions.
 */
export class IdempotencyManager {
  constructor(ttlMs = 86400000) {
    this.ttlMs = ttlMs;
    // Fast in-memory map: key -> { state: 'PENDING'|'COMMITTED'|'FAILED', response: any, expiresAt: number, createdAt: number }
    this.store = new Map();
    this.hits = 0;
    this.misses = 0;
    this.conflicts = 0;

    // Periodic cleanup of expired idempotency keys (every 30 seconds)
    this.cleanupTimer = setInterval(() => this._cleanupExpired(), 30000);
    this.cleanupTimer.unref();
  }

  /**
   * Tries to acquire an idempotency lock for a key.
   * Returns:
   *  { status: 'NEW' } -> Proceed with processing
   *  { status: 'CACHED', response: ... } -> Return cached response immediately
   *  { status: 'IN_FLIGHT' } -> 409 Conflict, previous request still processing
   */
  acquire(idempotencyKey) {
    if (!idempotencyKey) {
      return { status: 'NEW' };
    }

    const now = Date.now();
    const existing = this.store.get(idempotencyKey);

    if (existing) {
      if (existing.expiresAt > now) {
        if (existing.state === 'PENDING') {
          this.conflicts++;
          return { status: 'IN_FLIGHT' };
        }
        this.hits++;
        return { 
          status: 'CACHED', 
          response: existing.response, 
          httpStatus: existing.httpStatus || 200,
          cachedAt: existing.createdAt 
        };
      } else {
        // Expired key, remove it
        this.store.delete(idempotencyKey);
      }
    }

    // Set lock as PENDING
    this.misses++;
    this.store.set(idempotencyKey, {
      state: 'PENDING',
      response: null,
      httpStatus: null,
      createdAt: now,
      expiresAt: now + this.ttlMs
    });

    return { status: 'NEW' };
  }

  /**
   * Finalizes the transaction and saves the response payload for future replays.
   */
  commit(idempotencyKey, responsePayload, httpStatus = 200) {
    if (!idempotencyKey) return;
    const item = this.store.get(idempotencyKey);
    if (item) {
      item.state = 'COMMITTED';
      item.response = responsePayload;
      item.httpStatus = httpStatus;
      item.committedAt = Date.now();
    }
  }

  /**
   * Marks key as failed and stores error response.
   */
  fail(idempotencyKey, errorPayload, httpStatus = 400) {
    if (!idempotencyKey) return;
    const item = this.store.get(idempotencyKey);
    if (item) {
      item.state = 'FAILED';
      item.response = errorPayload;
      item.httpStatus = httpStatus;
      item.failedAt = Date.now();
    }
  }

  /**
   * Releases lock immediately (e.g. on unhandled fatal error).
   */
  release(idempotencyKey) {
    if (idempotencyKey) {
      this.store.delete(idempotencyKey);
    }
  }

  _cleanupExpired() {
    const now = Date.now();
    for (const [key, val] of this.store.entries()) {
      if (val.expiresAt <= now) {
        this.store.delete(key);
      }
    }
  }

  getMetrics() {
    return {
      totalKeysTracked: this.store.size,
      cacheHits: this.hits,
      cacheMisses: this.misses,
      concurrentConflicts: this.conflicts
    };
  }
}

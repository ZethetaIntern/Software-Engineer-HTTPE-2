/**
 * Circuit Breaker pattern implementation for fault isolation and resilience.
 * Protects downstream databases, payment networks, and shard workers from cascading degradation.
 */
export class CircuitBreaker {
  constructor(name = 'default', options = {}) {
    this.name = name;
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeoutMs = options.resetTimeoutMs || 3000;
    this.halfOpenSuccessThreshold = options.halfOpenSuccessThreshold || 2;

    this.state = 'CLOSED'; // 'CLOSED' | 'OPEN' | 'HALF_OPEN'
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = 0;
    this.trippedCount = 0;
  }

  isOpen() {
    if (this.state === 'OPEN') {
      const now = Date.now();
      if (now - this.lastFailureTime > this.resetTimeoutMs) {
        this.state = 'HALF_OPEN';
        this.successCount = 0;
        return false;
      }
      return true;
    }
    return false;
  }

  recordSuccess() {
    if (this.state === 'HALF_OPEN') {
      this.successCount++;
      if (this.successCount >= this.halfOpenSuccessThreshold) {
        this.state = 'CLOSED';
        this.failureCount = 0;
      }
    } else if (this.state === 'CLOSED') {
      this.failureCount = 0;
    }
  }

  recordFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.state === 'HALF_OPEN' || this.failureCount >= this.failureThreshold) {
      if (this.state !== 'OPEN') {
        this.trippedCount++;
      }
      this.state = 'OPEN';
    }
  }

  async execute(actionFn, fallbackFn = null) {
    if (this.isOpen()) {
      if (fallbackFn) {
        return fallbackFn(new Error(`Circuit breaker '${this.name}' is OPEN`));
      }
      throw new Error(`Circuit breaker '${this.name}' is OPEN. Requests blocked to prevent cascading failure.`);
    }

    try {
      const result = await actionFn();
      this.recordSuccess();
      return result;
    } catch (err) {
      this.recordFailure();
      if (fallbackFn) {
        return fallbackFn(err);
      }
      throw err;
    }
  }

  getMetrics() {
    return {
      name: this.name,
      state: this.state,
      failureCount: this.failureCount,
      trippedCount: this.trippedCount,
      lastFailureTime: this.lastFailureTime
    };
  }

  reset() {
    this.state = 'CLOSED';
    this.failureCount = 0;
    this.successCount = 0;
  }

  forceTrip() {
    this.state = 'OPEN';
    this.lastFailureTime = Date.now();
    this.trippedCount++;
  }
}

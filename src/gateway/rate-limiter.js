/**
 * High-performance Sliding-Window Rate Limiter.
 * Protects downstream payment shards from DDoS and sudden traffic spikes.
 */
export class SlidingWindowRateLimiter {
  constructor(maxTps = 20000, windowMs = 1000) {
    this.maxTps = maxTps;
    this.windowMs = windowMs;
    // Map: clientId -> { tokens: number, lastRefill: number }
    this.buckets = new Map();
    this.totalAllowed = 0;
    this.totalThrottled = 0;
  }

  isAllowed(clientId = 'default') {
    const now = Date.now();
    let bucket = this.buckets.get(clientId);

    if (!bucket) {
      bucket = {
        tokens: this.maxTps,
        lastRefill: now
      };
      this.buckets.set(clientId, bucket);
    }

    // Refill tokens based on elapsed time
    const elapsed = now - bucket.lastRefill;
    if (elapsed > 0) {
      const tokensToAdd = (elapsed / this.windowMs) * this.maxTps;
      bucket.tokens = Math.min(this.maxTps, bucket.tokens + tokensToAdd);
      bucket.lastRefill = now;
    }

    if (bucket.tokens >= 1) {
      bucket.tokens -= 1;
      this.totalAllowed++;
      return { allowed: true, remaining: Math.floor(bucket.tokens) };
    } else {
      this.totalThrottled++;
      return { allowed: false, remaining: 0 };
    }
  }

  setMaxTps(newMax) {
    this.maxTps = newMax;
  }

  getMetrics() {
    return {
      maxTps: this.maxTps,
      totalAllowed: this.totalAllowed,
      totalThrottled: this.totalThrottled,
      trackedClients: this.buckets.size
    };
  }
}

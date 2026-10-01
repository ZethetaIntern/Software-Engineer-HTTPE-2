export const CONFIG = {
  PORT: process.env.PORT || 8080,
  HOST: process.env.HOST || '0.0.0.0',
  NUM_SHARDS: parseInt(process.env.NUM_SHARDS || '8', 10),
  VIRTUAL_NODES_PER_SHARD: 128,
  
  // Rate Limiting
  RATE_LIMIT_MAX_TPS: parseInt(process.env.RATE_LIMIT_MAX_TPS || '20000', 10),
  RATE_LIMIT_WINDOW_MS: 1000,
  
  // Idempotency
  IDEMPOTENCY_TTL_SECONDS: 86400, // 24 hours
  IDEMPOTENCY_LOCK_TIMEOUT_MS: 5000,
  
  // Storage & Batching
  OUTBOX_FLUSH_INTERVAL_MS: 10,
  OUTBOX_BATCH_SIZE: 1000,
  
  // Resilience
  CIRCUIT_BREAKER_FAILURE_THRESHOLD: 5,
  CIRCUIT_BREAKER_RESET_TIMEOUT_MS: 3000,
  
  // Redis & Kafka (optional external connections, defaults to high-speed in-memory engine)
  REDIS_URL: process.env.REDIS_URL || null,
  KAFKA_BROKERS: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : null
};

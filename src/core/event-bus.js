import { Kafka } from 'kafkajs';

/**
 * Event-Driven Messaging Layer (Kafka + Transactional Outbox + Dead Letter Queue).
 * Supports native Kafka connection or ultra-high-speed embedded in-memory bus for local execution.
 */
export class EventBus {
  constructor(options = {}) {
    this.kafkaBrokers = options.kafkaBrokers || null;
    this.outboxBatchSize = options.outboxBatchSize || 500;
    this.outboxFlushIntervalMs = options.outboxFlushIntervalMs || 10;
    
    // In-memory topics
    this.topics = {
      'txn.initiated': [],
      'txn.settled': [],
      'txn.failed': [],
      'txn.dlq': []
    };

    // Subscriptions: topic -> Set of callback functions
    this.subscribers = new Map();

    // Outbox buffer for atomic outbox pattern
    this.outboxBuffer = [];
    
    // Telemetry
    this.metrics = {
      totalProduced: 0,
      totalConsumed: 0,
      totalOutboxFlushed: 0,
      dlqCount: 0
    };

    this.isFlushing = false;
    this.outboxTimer = setInterval(() => this.flushOutbox(), this.outboxFlushIntervalMs);
    this.outboxTimer.unref();
  }

  async initialize() {
    if (this.kafkaBrokers && this.kafkaBrokers.length > 0) {
      try {
        this.kafka = new Kafka({
          clientId: 'httpe-core-engine',
          brokers: this.kafkaBrokers,
          retry: { retries: 3 }
        });
        this.producer = this.kafka.producer({
          allowAutoTopicCreation: true,
          idempotent: true
        });
        await this.producer.connect();
        console.log('[EventBus] Connected to external Kafka brokers:', this.kafkaBrokers);
      } catch (err) {
        console.warn('[EventBus] Kafka connection failed, falling back to embedded event bus:', err.message);
        this.producer = null;
      }
    }
  }

  /**
   * Enqueues an event into the transactional outbox buffer.
   */
  queueOutbox(topic, key, payload) {
    this.outboxBuffer.push({
      topic,
      key,
      payload,
      timestamp: Date.now()
    });

    if (this.outboxBuffer.length >= this.outboxBatchSize) {
      this.flushOutbox();
    }
  }

  /**
   * Flushes outbox buffer to Kafka / Event Bus in high-velocity batches.
   */
  async flushOutbox() {
    if (this.isFlushing || this.outboxBuffer.length === 0) return;
    this.isFlushing = true;

    const batch = this.outboxBuffer.splice(0, this.outboxBatchSize);
    try {
      if (this.producer) {
        const recordsByTopic = new Map();
        for (const item of batch) {
          if (!recordsByTopic.has(item.topic)) {
            recordsByTopic.set(item.topic, []);
          }
          recordsByTopic.get(item.topic).push({
            key: String(item.key),
            value: JSON.stringify(item.payload)
          });
        }

        const topicMessages = Array.from(recordsByTopic.entries()).map(([topic, messages]) => ({
          topic,
          messages
        }));

        await this.producer.sendBatch({ topicMessages });
      } else {
        // Embedded event bus dispatch
        for (const item of batch) {
          this._dispatchLocal(item.topic, item.key, item.payload);
        }
      }

      this.metrics.totalProduced += batch.length;
      this.metrics.totalOutboxFlushed += batch.length;
    } catch (err) {
      console.error('[EventBus] Outbox batch send error:', err.message);
      // Route unrecoverable errors to Dead Letter Queue (DLQ)
      this.routeToDLQ('outbox.batch.error', batch, err.message);
    } finally {
      this.isFlushing = false;
    }
  }

  _dispatchLocal(topic, key, payload) {
    if (!this.topics[topic]) {
      this.topics[topic] = [];
    }
    // Keep last 1,000 events in memory for audit & telemetry
    if (this.topics[topic].length > 1000) {
      this.topics[topic].shift();
    }
    this.topics[topic].push({ key, payload, timestamp: Date.now() });

    const subs = this.subscribers.get(topic);
    if (subs) {
      for (const cb of subs) {
        try {
          cb({ topic, key, payload });
          this.metrics.totalConsumed++;
        } catch (subErr) {
          this.routeToDLQ(topic, { key, payload }, subErr.message);
        }
      }
    }
  }

  subscribe(topic, callback) {
    if (!this.subscribers.has(topic)) {
      this.subscribers.set(topic, new Set());
    }
    this.subscribers.get(topic).add(callback);
  }

  routeToDLQ(sourceTopic, payload, errorReason) {
    this.metrics.dlqCount++;
    const dlqRecord = {
      sourceTopic,
      payload,
      errorReason,
      timestamp: Date.now()
    };
    if (this.topics['txn.dlq'].length > 1000) {
      this.topics['txn.dlq'].shift();
    }
    this.topics['txn.dlq'].push(dlqRecord);
  }

  getMetrics() {
    return {
      ...this.metrics,
      outboxBacklog: this.outboxBuffer.length,
      topicsCount: Object.keys(this.topics).length,
      dlqEventsStored: this.topics['txn.dlq'] ? this.topics['txn.dlq'].length : 0
    };
  }

  getRecentEvents(topic = 'txn.settled', limit = 20) {
    const list = this.topics[topic] || [];
    return list.slice(-limit).reverse();
  }
}

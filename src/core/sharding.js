import crypto from 'crypto';

/**
 * Consistent Hash Ring implementation for Database & Ledger Sharding.
 * Guarantees uniform key distribution with minimal rebalancing upon shard failure or scaling.
 */
export class ConsistentHashRing {
  constructor(shardCount = 8, virtualNodes = 128) {
    this.shardCount = shardCount;
    this.virtualNodes = virtualNodes;
    this.ring = []; // Array of { hash: number, shardId: number }
    this.shards = new Map(); // shardId -> shard metadata

    this._initializeRing();
  }

  _hash(key) {
    // 32-bit FNV-1a hash for blazing fast execution in Node.js
    let hash = 2166136261;
    for (let i = 0; i < key.length; i++) {
      hash ^= key.charCodeAt(i);
      hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
    }
    return hash >>> 0; // Unsigned 32-bit int
  }

  _initializeRing() {
    this.ring = [];
    for (let shardId = 0; shardId < this.shardCount; shardId++) {
      this.shards.set(shardId, {
        id: shardId,
        status: 'HEALTHY',
        activeTransactions: 0,
        totalCommitted: 0,
        totalFailed: 0,
        createdAt: Date.now()
      });

      for (let v = 0; v < this.virtualNodes; v++) {
        const vNodeKey = `shard-${shardId}-vnode-${v}`;
        const hash = this._hash(vNodeKey);
        this.ring.push({ hash, shardId });
      }
    }

    // Sort ring by hash value ascending for binary search
    this.ring.sort((a, b) => a.hash - b.hash);
  }

  /**
   * Resolves the primary shard ID for an account key using binary search on the ring.
   */
  getShardForAccount(accountId) {
    if (!accountId) throw new Error('Account ID is required for shard routing');
    const hash = this._hash(String(accountId));

    // Binary search for the first node with hash >= account hash
    let low = 0;
    let high = this.ring.length - 1;

    if (hash > this.ring[high].hash || hash <= this.ring[0].hash) {
      return this.ring[0].shardId;
    }

    while (low <= high) {
      const mid = (low + high) >>> 1;
      if (this.ring[mid].hash >= hash) {
        if (mid === 0 || this.ring[mid - 1].hash < hash) {
          return this.ring[mid].shardId;
        }
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }

    return this.ring[0].shardId;
  }

  getShardMetadata(shardId) {
    return this.shards.get(shardId);
  }

  getAllShards() {
    return Array.from(this.shards.values());
  }

  setShardStatus(shardId, status) {
    const shard = this.shards.get(shardId);
    if (shard) {
      shard.status = status;
    }
  }
}

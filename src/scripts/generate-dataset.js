import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { v4 as uuidv4 } from 'uuid';
import { ConsistentHashRing } from '../core/sharding.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATASET_DIR = path.join(__dirname, '..', '..', 'dataset');

const COUNT = parseInt(process.argv[2] || '1000', 10);
const NUM_ACCOUNTS = 100;
const ring = new ConsistentHashRing(8, 128);

console.log(`Generating ${COUNT} synthetic transactions into: ${DATASET_DIR}`);

if (!fs.existsSync(DATASET_DIR)) {
  fs.mkdirSync(DATASET_DIR, { recursive: true });
}

// Generate Accounts
const accounts = [];
for (let i = 1; i <= NUM_ACCOUNTS; i++) {
  const accId = `ACC-${String(i).padStart(4, '0')}`;
  accounts.push({
    accountId: accId,
    shardId: ring.getShardForAccount(accId),
    balance: 500000.00,
    currency: 'USD',
    status: 'ACTIVE',
    createdAt: new Date().toISOString()
  });
}

fs.writeFileSync(path.join(DATASET_DIR, 'accounts.json'), JSON.stringify(accounts, null, 2));

// Generate Transactions
const txns = [];
const csvHeader = 'txn_id,idempotency_key,source_account_id,destination_account_id,amount,currency,type,source_shard_id,dest_shard_id,is_cross_shard,status,timestamp\n';
let csvContent = csvHeader;

const types = ['P2P_TRANSFER', 'MERCHANT_PAYMENT', 'REFUND', 'ESCROW_RELEASE', 'SETTLEMENT'];

for (let i = 0; i < COUNT; i++) {
  const sIdx = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  let dIdx = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  while (dIdx === sIdx) {
    dIdx = Math.floor(Math.random() * NUM_ACCOUNTS) + 1;
  }

  const src = `ACC-${String(sIdx).padStart(4, '0')}`;
  const dst = `ACC-${String(dIdx).padStart(4, '0')}`;
  const srcShard = ring.getShardForAccount(src);
  const dstShard = ring.getShardForAccount(dst);
  const amount = (Math.random() * 500 + 10).toFixed(2);
  const txnId = `TXN-${Date.now()}-${uuidv4().substring(0, 8).toUpperCase()}`;
  const idempKey = `IDEMP-${uuidv4()}`;
  const type = types[Math.floor(Math.random() * types.length)];
  const isCross = srcShard !== dstShard;
  const ts = new Date(Date.now() - Math.floor(Math.random() * 86400000)).toISOString();

  const record = {
    txnId,
    idempotencyKey: idempKey,
    sourceAccountId: src,
    destinationAccountId: dst,
    amount: parseFloat(amount),
    currency: 'USD',
    type,
    sourceShardId: srcShard,
    destShardId: dstShard,
    isCrossShard: isCross,
    status: 'COMMITTED',
    timestamp: ts
  };

  txns.push(record);
  csvContent += `${txnId},${idempKey},${src},${dst},${amount},USD,${type},${srcShard},${dstShard},${isCross},COMMITTED,${ts}\n`;
}

fs.writeFileSync(path.join(DATASET_DIR, 'transactions.json'), JSON.stringify(txns, null, 2));
fs.writeFileSync(path.join(DATASET_DIR, 'transactions.csv'), csvContent);

console.log(`✅ Successfully generated datasets in ${DATASET_DIR}:`);
console.log(`   - accounts.json (${accounts.length} accounts)`);
console.log(`   - transactions.json (${txns.length} transactions)`);
console.log(`   - transactions.csv (${txns.length} records)`);

# Capacity Planning & Budget Optimization (12,000+ TPS)
## High-Throughput Transaction Processing Engine (HTTPE)

---

### 1. Workload Characterization & Sizing Assumptions

| Metric | Target Specification | Notes |
| :--- | :--- | :--- |
| **Peak Throughput** | **12,000 – 15,000 TPS** | Sustained load during festival/flash sale peaks |
| **Average Throughput** | 4,000 TPS | Normal banking business hours |
| **Payload Size (Request)** | ~450 Bytes | JSON schema with auth headers, accounts, signature |
| **Payload Size (Response)** | ~350 Bytes | JSON transaction receipt & balance confirmation |
| **Idempotency Window** | 24 Hours (86,400s) | Prevents duplicate debit execution |
| **Daily Transactions** | ~350,000,000 tx/day | Total daily ledger volume |

---

### 2. Resource & Hardware Calculations

#### A. Network Bandwidth Requirements
$$\text{Ingress Traffic} = 15,000 \times 450 \text{ Bytes} = 6.75 \text{ MB/s} = 54 \text{ Mbps}$$
$$\text{Egress Traffic} = 15,000 \times 350 \text{ Bytes} = 5.25 \text{ MB/s} = 42 \text{ Mbps}$$
$$\text{Total Network Throughput} \approx 12 \text{ MB/s} = 96 \text{ Mbps}$$
- **Provisioned Bandwidth:** 1 Gbps to 10 Gbps enhanced networking instance family (e.g. AWS `c6i.2xlarge` or GCP `c2-standard-8`) provides massive headroom (>10x).

#### B. Memory (RAM) Sizing for 24h Idempotency & Active Accounts
1. **Active Accounts in L1/L2 Cache:**
   - 10,000,000 hot accounts @ 128 bytes/account $\approx 1.28 \text{ GB}$
2. **Idempotency Keys (24-Hour Sliding Retention):**
   - At peak 12,000 TPS sustained for 24h:
     $$12,000 \times 86,400 = 1,036,800,000 \text{ records}$$
   - Using Redis Bloom Filter (probabilistic 0.1% false-positive rate): ~1.2 GB
   - Storing canonical 64-bit UUID hash + state pointer in Redis Cluster (compressed key-value):
     $$10^9 \times 48 \text{ bytes} \approx 48 \text{ GB}$$
   - **Recommended Redis Cluster Memory:** 64 GB distributed across 4 nodes (16 GB per node).

#### C. Compute (CPU Cores)
- Each transaction requires:
  - HMAC-SHA256 signature verification: ~15 $\mu$s
  - Consistent Hash Ring resolution: ~1 $\mu$s
  - Double-entry balance calculation: ~2 $\mu$s
  - Outbox serialization: ~10 $\mu$s
  - Total CPU time per transaction: $\approx 35\ \mu\text{s} = 0.035\text{ ms}$
- CPU capacity per physical core:
  $$\frac{1,000 \text{ ms}}{0.035 \text{ ms}} \approx 28,500 \text{ ops/sec}$$
- With 50% target CPU utilization safety margin:
  $$\text{Required vCPUs} = \frac{15,000 \text{ TPS}}{14,000 \text{ TPS/core}} \approx 1.07 \text{ cores}$$
- **Deployment Recommendation:** 4 Node.js cluster worker instances with 4 vCPUs each (16 vCPUs total) for N+2 fault tolerance and sub-millisecond event loops.

#### D. Storage & Disk IOPS (Write-Ahead Log & Kafka)
- **Transactional Write Volume:**
  $$15,000 \text{ writes/sec} \times 256 \text{ bytes/journal} \approx 3.84 \text{ MB/sec}$$
- **Sequential Disk Writes:** Standard NVMe SSD sustains 250+ MB/sec write throughput.
- **IOPS Requirement:** Group commit batching of 100 tx/flush yields 150 IOPS write load. Provisioned GP3 / io2 volume with 3,000 IOPS provides 20x safety buffer.

---

### 3. Scalable Cloud Budget Breakdown (Fintech Tier)

Optimized for high availability without excessive enterprise sprawl:

| Component | AWS Sizing & Instance Type | GCP Sizing & Instance Type | Monthly Cost (USD) |
| :--- | :--- | :--- | :--- |
| **API Gateway & Core Shards** | 4 $\times$ `c6i.xlarge` (4 vCPU, 8 GB RAM) | 4 $\times$ `c2-standard-4` (4 vCPU, 16 GB) | ~$390 / month |
| **Redis In-Memory Cluster** | 3 $\times$ `cache.r6g.large` (13.07 GB RAM) | Cloud Memorystore 32 GB HA | ~$270 / month |
| **Event Bus (Kafka / Redpanda)** | 3-node Redpanda Serverless or `m6g.large` | 3 $\times$ `e2-standard-4` | ~$220 / month |
| **Ledger Storage (PostgreSQL Aurora)**| `db.r6g.xlarge` Multi-AZ with Read Replicas | Cloud SQL Enterprise Plus | ~$480 / month |
| **Network Egress & Load Balancer** | AWS ALB + 5 TB Egress | Cloud Load Balancing + 5 TB | ~$140 / month |
| **Monitoring & Telemetry** | Managed Prometheus / Grafana | Cloud Monitoring | ~$60 / month |
| **TOTAL ESTIMATED CLOUD BUDGET** | | | **~$1,560 / month** |

> **Cost Efficiency:** At 350M transactions/day ($10.5\text{B}$ tx/month), the infrastructure cost is **$0.000000148 per transaction** (< 15 cents per 1,000,000 transactions), easily fitting into tight startup or BFSI enterprise constraints.

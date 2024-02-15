# distributed-backend-toolkit

Reusable distributed backend infrastructure patterns in Java 21 / Spring Boot 3. Covers Kafka partition-aware scaling, two-tier Redis caching, async processing with CompletableFuture, and high-throughput PostgreSQL batch operations — each pattern documented with the engineering rationale behind the design choices.

---

## Patterns Included

### 1. Kafka — Partition-aware producer + batch consumer

**`kafka/PartitionAwareProducer.java`**
- Keys every message by entity ID (device_id) → deterministic partition routing via murmur2 hash
- Idempotent producer (`enable.idempotence=true`) prevents duplicate writes on retry
- Micro-batching: `linger.ms=5` + `batch.size=64KB` + LZ4 compression for throughput
- Async send via `CompletableFuture` — non-blocking with structured error logging

**`kafka/BatchConsumer.java`**
- Batch listener: receives `List<ConsumerRecord>` instead of one record at a time
- `AckMode.BATCH`: commits offset only after entire batch is processed and persisted
- Concurrency=3 (configurable): one thread per 2 partitions, scales to 6 for max throughput
- Graceful degradation: deserialization errors skip the record, don't fail the batch

**Why partition affinity?**
With device events keyed by device_id, all reads for a given device land on one partition, consumed by one thread — stateful in-memory processing (sliding windows, session state, LRU caches) with zero distributed coordination.

---

### 2. Redis — Two-tier cache (L1 Caffeine + L2 Redis)

**`cache/TieredCacheService.java`**

```
Read: L1 (Caffeine, ~0μs) → L2 (Redis, ~1ms) → Source DB → populate both tiers
Write: Update source → evict L1 + L2 (invalidation, not write-through)
```

| Tier | Implementation | Latency | Scope | TTL |
|---|---|---|---|---|
| L1 | Caffeine (heap) | < 1μs | Per JVM | 60s |
| L2 | Redis (Lettuce) | ~1ms | Shared | 5m |

- Invalidation on write (not write-through) avoids cache coherency bugs in multi-instance deployments
- Atomic Redis `INCR` for distributed counters and rate limiting — O(1), no lock needed
- Metrics: L1 hit rate, L2 hit rate, miss rate via Micrometer counters

---

### 3. Async — CompletableFuture pipeline patterns

**`async/AsyncTaskProcessor.java`**

Three patterns demonstrated:

**Fan-out/fan-in**: Process N tasks in parallel, join on `allOf()`:
```java
CompletableFuture.allOf(futures.toArray(...)).thenApply(v -> collectResults())
```

**Pipeline composition**: Chain stages without blocking threads between them:
```java
validate(input)
  .thenApplyAsync(this::enrich)
  .exceptionally(ex -> degradedResult)  // enrich failed — don't fail the pipeline
  .thenApplyAsync(this::transform)
```

**CallerRunsPolicy**: When the task queue is full, the calling thread executes the task — natural backpressure without data loss (vs `AbortPolicy` which throws and loses work).

---

### 4. PostgreSQL — High-throughput batch operations

**`db/BatchRepository.java`**

- `JdbcTemplate.batchUpdate()` with page size=500: ~500x fewer DB round-trips vs individual inserts
- `ON CONFLICT DO NOTHING`: idempotent inserts — safe for Kafka at-least-once redelivery
- `@Transactional(readOnly=true)`: routes read queries to replica when HikariCP is configured for read/write split
- Partition pruning: all queries include `event_timestamp` in WHERE clause — PostgreSQL scans only the relevant monthly partition

**`db/schema.sql`** index strategy:
```sql
-- Composite B-tree: covers device + time queries (index-only scan, no heap access)
CREATE INDEX idx_events_device_time ON events (device_id, event_timestamp DESC);

-- Partial index: only critical events — tiny, fast for dashboard queries
CREATE INDEX idx_events_critical ON events (event_timestamp DESC)
WHERE event_type = 'critical';

-- GIN index: JSONB payload for ad-hoc field queries
CREATE INDEX idx_events_payload ON events USING GIN (payload);
```

---

## Getting Started

```bash
git clone https://github.com/vinaymohan768/distributed-backend-toolkit
cd distributed-backend-toolkit
docker compose up --build
```

API at `http://localhost:8080` · Actuator metrics at `http://localhost:8080/actuator/prometheus`

---

## Benchmark Endpoints

Each pattern has a built-in benchmark endpoint to measure real numbers:

```bash
# Kafka: produce 1000 events, report throughput
curl -X POST "http://localhost:8080/api/v1/benchmark/kafka/produce?count=1000&deviceCount=10"

# Cache: 10K reads, report L1 latency
curl "http://localhost:8080/api/v1/benchmark/cache?reads=10000"

# DB: batch insert 5000 rows, report throughput
curl -X POST "http://localhost:8080/api/v1/benchmark/db/batch-insert?rows=5000"

# Async: fan-out 200 tasks in parallel
curl -X POST "http://localhost:8080/api/v1/benchmark/async/parallel?taskCount=200"

# Query recent device events
curl "http://localhost:8080/api/v1/devices/DEV-00001/events?limit=50"
```

---

## Project Structure

```
distributed-backend-toolkit/
├── src/main/java/com/vinay/toolkit/
│   ├── config/
│   │   ├── KafkaConfig.java        # Producer + consumer factory, topic declarations
│   │   ├── RedisConfig.java        # Lettuce connection, String serialization
│   │   └── AsyncConfig.java        # ThreadPoolTaskExecutor, CallerRunsPolicy
│   ├── kafka/
│   │   ├── PartitionAwareProducer  # Key-based routing, idempotent, async send
│   │   └── BatchConsumer           # Batch listener, manual AckMode.BATCH
│   ├── cache/
│   │   └── TieredCacheService      # L1 Caffeine + L2 Redis, invalidation pattern
│   ├── db/
│   │   └── BatchRepository         # batchUpdate, partition pruning, readOnly tx
│   ├── async/
│   │   └── AsyncTaskProcessor      # Fan-out, pipeline, backpressure
│   └── api/
│       └── BenchmarkController     # Runnable benchmarks for each pattern
├── db/
│   └── schema.sql                  # Partitioned table, composite + partial + GIN indexes
├── docker-compose.yml              # Kafka + PostgreSQL + Redis + App
├── Dockerfile                      # Multi-stage Maven build
└── pom.xml                         # Spring Boot 3, Kafka, Redis, Resilience4j
```

---

## Tech Stack

`Java 21` `Spring Boot 3.2` `Apache Kafka` `Redis (Lettuce)` `PostgreSQL 16` `HikariCP` `Caffeine` `Resilience4j` `Micrometer` `Docker Compose`

# Distributed Backend Toolkit

![CI](https://github.com/vinaymohan768/distributed-backend-toolkit/actions/workflows/ci.yml/badge.svg)
![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2-6DB33F?logo=springboot&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?logo=apachekafka&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

Production-grade distributed backend patterns in Java 21 / Spring Boot 3. Four self-contained modules: Kafka partition-aware scaling, two-tier Redis caching, async CompletableFuture pipelines, and high-throughput PostgreSQL batch operations: each with runnable benchmarks and the engineering rationale behind every design choice.

---

## Patterns

### 1. Kafka: Partition-aware producer + batch consumer

```
Producer → murmur2(device_id) → deterministic partition → ordered per device
Consumer → batch poll (max 500 records) → process → persist → ack offset
```

**`kafka/PartitionAwareProducer.java`**
- Keyed by entity ID → murmur2 hash → same partition, guaranteed per-entity ordering
- Idempotent producer (`enable.idempotence=true`): no duplicates on broker retry
- `linger.ms=5` + `batch.size=64KB` + LZ4: micro-batching for throughput without sacrificing latency
- Non-blocking async send via `CompletableFuture` with structured error logging

**`kafka/BatchConsumer.java`**
- `@KafkaListener` batch mode: receives `List<ConsumerRecord>` per poll cycle
- `AckMode.BATCH`: commits offset only after the entire batch is persisted; Kafka redelivers on crash
- Deserialization errors skip the bad record without dropping the rest of the batch
- `concurrency=3` → scale to 6 (one thread per partition) for max throughput

**Why partition affinity?** All events for `device-X` land on one partition, consumed by one thread. Stateful in-memory processing (sliding windows, session state) with zero distributed coordination.

---

### 2. Redis: Two-tier cache (L1 Caffeine + L2 Redis)

**`cache/TieredCacheService.java`**

```
Read:  L1 Caffeine (<1μs) → L2 Redis (~1ms) → source DB → populate both tiers
Write: update source → invalidate L1 + L2
```

| Tier | Implementation | Latency | Scope | TTL |
|------|---------------|---------|-------|-----|
| L1   | Caffeine (heap) | < 1μs | Per JVM | 60s |
| L2   | Redis (Lettuce) | ~1ms  | Shared  | 5m  |

- **Invalidation on write** (not write-through): avoids cache coherency bugs when multiple service instances share L2
- **Atomic Redis `INCR`** for rate limiting counters: O(1), no distributed lock
- Micrometer counters for L1 hit rate, L2 hit rate, and miss rate via `/actuator/prometheus`

---

### 3. Async: CompletableFuture pipeline patterns

**`async/AsyncTaskProcessor.java`**

**Fan-out/fan-in**: parallel task processing, join on `allOf()`:
```java
CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
    .thenApply(v -> futures.stream().map(CompletableFuture::join).toList());
```

**Pipeline composition**: chain stages without blocking threads between them:
```java
CompletableFuture.supplyAsync(() -> validate(input))
    .thenApplyAsync(this::enrich)
    .exceptionally(ex -> input)      // enrich failed → degrade gracefully
    .thenApplyAsync(this::transform);
```

**CallerRunsPolicy**: when the task queue fills up, the calling thread executes the task instead of throwing `RejectedExecutionException`. Natural backpressure without data loss.

---

### 4. PostgreSQL: High-throughput batch operations

**`db/BatchRepository.java`**

- `JdbcTemplate.batchUpdate()` at `page_size=500`: ~500x fewer round-trips vs individual inserts
- `ON CONFLICT DO NOTHING`: idempotent inserts, safe for Kafka at-least-once redelivery
- `@Transactional(readOnly=true)`: routes reads to replica when HikariCP is configured for read/write split
- `event_timestamp` in every WHERE clause → PostgreSQL partition pruning on monthly range partitions

**`db/schema.sql`** index strategy:
```sql
-- Composite B-tree: covers (device_id, time) queries: index-only scan, no heap read
CREATE INDEX idx_events_device_time ON events (device_id, event_timestamp DESC);

-- Partial index: only critical events: tiny index, fast dashboard queries
CREATE INDEX idx_events_critical ON events (event_timestamp DESC)
WHERE event_type = 'critical';

-- GIN index: JSONB payload for ad-hoc field queries
CREATE INDEX idx_events_payload ON events USING GIN (payload);
```

---

## Getting Started

**Requirements:** Docker, Docker Compose, Java 21 (for local dev)

```bash
git clone https://github.com/vinaymohan768/distributed-backend-toolkit
cd distributed-backend-toolkit
docker compose up --build
```

API at `http://localhost:8080` · Metrics at `http://localhost:8080/actuator/prometheus`

---

## Benchmark Endpoints

Each pattern has a built-in benchmark: run real numbers against your own machine:

```bash
# Kafka: produce 1000 events across 10 devices, report throughput
curl -X POST "http://localhost:8080/api/v1/benchmark/kafka/produce?count=1000&deviceCount=10"

# Cache: 10K reads, report L1 vs L2 hit rate
curl "http://localhost:8080/api/v1/benchmark/cache?reads=10000"

# DB: batch insert 5000 rows, report rows/sec
curl -X POST "http://localhost:8080/api/v1/benchmark/db/batch-insert?rows=5000"

# Async: fan-out 200 parallel tasks, report wall-clock vs sequential time
curl -X POST "http://localhost:8080/api/v1/benchmark/async/parallel?taskCount=200"

# Query recent events for a device
curl "http://localhost:8080/api/v1/devices/DEV-00001/events?limit=50"
```

---

## Project Structure

```
distributed-backend-toolkit/
├── src/main/java/com/vinay/toolkit/
│   ├── config/
│   │   ├── KafkaConfig.java          # Producer factory, consumer factory, topic declarations
│   │   ├── RedisConfig.java          # Lettuce connection pool, String serialization
│   │   └── AsyncConfig.java          # ThreadPoolTaskExecutor, CallerRunsPolicy
│   ├── kafka/
│   │   ├── PartitionAwareProducer.java  # Key-based routing, idempotent, async send
│   │   └── BatchConsumer.java           # Batch listener, AckMode.BATCH, error isolation
│   ├── cache/
│   │   └── TieredCacheService.java   # L1 Caffeine + L2 Redis, invalidation, rate limiting
│   ├── db/
│   │   └── BatchRepository.java      # batchUpdate, ON CONFLICT, partition pruning, readOnly tx
│   ├── async/
│   │   └── AsyncTaskProcessor.java   # Fan-out, pipeline, CallerRunsPolicy backpressure
│   └── api/
│       └── BenchmarkController.java  # Runnable benchmarks for each pattern
├── db/
│   └── schema.sql                    # Partitioned table, composite + partial + GIN indexes
├── src/main/resources/
│   └── application.yml               # Full HikariCP, Kafka, Redis, async config
├── docker-compose.yml                # Kafka + Zookeeper + PostgreSQL + Redis + App
├── Dockerfile                        # Multi-stage Maven build, non-root, G1GC flags
└── pom.xml
```

---

## Stack

`Java 21` · `Spring Boot 3.2` · `Apache Kafka 3.7` · `Redis 7 (Lettuc
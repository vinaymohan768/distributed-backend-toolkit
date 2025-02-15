package com.vinay.toolkit.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.vinay.toolkit.async.AsyncTaskProcessor;
import com.vinay.toolkit.cache.TieredCacheService;
import com.vinay.toolkit.db.BatchRepository;
import com.vinay.toolkit.kafka.PartitionAwareProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Benchmark and demonstration endpoints.
 *
 * These endpoints make it easy to test each toolkit component independently
 * and observe throughput / latency numbers. In a real service, these would
 * be removed or secured behind an admin auth layer.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class BenchmarkController {

    private final PartitionAwareProducer producer;
    private final TieredCacheService cache;
    private final BatchRepository repository;
    private final AsyncTaskProcessor asyncProcessor;

    // ── Health ────────────────────────────────────────────────────────────────

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "ok",
                "service", "distributed-backend-toolkit",
                "timestamp", System.currentTimeMillis()
        );
    }

    // ── Kafka benchmarks ──────────────────────────────────────────────────────

    /**
     * Send N events to Kafka and report throughput.
     * Events are keyed by device_id: partition affinity in action.
     */
    @PostMapping("/benchmark/kafka/produce")
    public Map<String, Object> benchmarkProduce(
            @RequestParam(defaultValue = "1000") int count,
            @RequestParam(defaultValue = "10") int deviceCount) {

        long start = System.currentTimeMillis();

        List<Map.Entry<String, Object>> events = IntStream.range(0, count)
                .mapToObj(i -> {
                    String deviceId = "DEV-" + String.format("%05d", i % deviceCount);
                    Map<String, Object> payload = Map.of(
                            "device_id", deviceId,
                            "cpu", 40 + (i % 50),
                            "timestamp", System.currentTimeMillis()
                    );
                    return Map.entry(deviceId, (Object) payload);
                })
                .toList();

        producer.sendBatch("device-events", events).join();
        long elapsed = System.currentTimeMillis() - start;

        return Map.of(
                "events_sent", count,
                "elapsed_ms", elapsed,
                "throughput_per_sec", count * 1000L / Math.max(elapsed, 1)
        );
    }

    // ── Cache benchmarks ──────────────────────────────────────────────────────

    /**
     * Populate the cache and measure read latency across cache tiers.
     */
    @GetMapping("/benchmark/cache")
    public Map<String, Object> benchmarkCache(@RequestParam(defaultValue = "1000") int reads) {
        String key = "benchmark:device:DEV-00001";
        Map<String, Object> value = Map.of("device_id", "DEV-00001", "model", "SD8Gen3");

        cache.put(key, value);

        // L1 warm reads
        long start = System.nanoTime();
        for (int i = 0; i < reads; i++) {
            cache.get(key, new TypeReference<Map<String, Object>>() {}, () -> value);
        }
        long l1LatencyNs = (System.nanoTime() - start) / reads;

        cache.evict(key);

        return Map.of(
                "reads", reads,
                "avg_l1_latency_ns", l1LatencyNs,
                "avg_l1_latency_us", l1LatencyNs / 1000.0
        );
    }

    // ── DB benchmarks ─────────────────────────────────────────────────────────

    /**
     * Insert N rows in batch and report throughput.
     */
    @PostMapping("/benchmark/db/batch-insert")
    public Map<String, Object> benchmarkBatchInsert(
            @RequestParam(defaultValue = "5000") int rows) {

        Random rand = new Random();
        List<Map<String, Object>> events = IntStream.range(0, rows)
                .mapToObj(i -> (Map<String, Object>) new HashMap<String, Object>() {{
                    put("device_id", "DEV-" + String.format("%05d", rand.nextInt(50)));
                    put("event_type", rand.nextInt(10) < 1 ? "critical" : "normal");
                    put("payload", "{\"cpu\":" + (30 + rand.nextInt(60)) + "}");
                    put("event_timestamp", new java.sql.Timestamp(
                            System.currentTimeMillis() - rand.nextInt(3600_000)).toInstant().toString());
                }})
                .collect(Collectors.toList());

        long start = System.currentTimeMillis();
        int inserted = repository.insertEventsBatch(events);
        long elapsed = System.currentTimeMillis() - start;

        return Map.of(
                "rows_attempted", rows,
                "rows_inserted", inserted,
                "elapsed_ms", elapsed,
                "throughput_per_sec", rows * 1000L / Math.max(elapsed, 1)
        );
    }

    // ── Async benchmarks ──────────────────────────────────────────────────────

    /**
     * Fan-out N tasks across the async thread pool and measure total time.
     */
    @PostMapping("/benchmark/async/parallel")
    public Map<String, Object> benchmarkAsync(
            @RequestParam(defaultValue = "100") int taskCount) throws Exception {

        List<Map<String, Object>> tasks = IntStream.range(0, taskCount)
                .mapToObj(i -> Map.<String, Object>of("id", "task-" + i, "index", i))
                .toList();

        long start = System.currentTimeMillis();
        List<Map<String, Object>> results = asyncProcessor
                .processInParallel(tasks)
                .get();
        long elapsed = System.currentTimeMillis() - start;

        return Map.of(
                "tasks", taskCount,
                "completed", results.size(),
                "elapsed_ms", elapsed,
                "avg_task_ms", elapsed / Math.max(taskCount, 1)
        );
    }

    // ── Query endpoints ───────────────────────────────────────────────────────

    @GetMapping("/devices/{deviceId}/events")
    public List<Map<String, Object>> getDeviceEvents(
            @PathVariable String deviceId,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "60") int sinceMinutes) {
        return repository.getRecentEvents(deviceId, limit, sinceMinutes);
    }

    @GetMapping("/stats")
    public List<Map<String, Object>> getStats(
            @RequestParam(defaultValue = "60") int windowMinutes) {
        return repository.getDeviceStats(windowMinutes);
    }
}

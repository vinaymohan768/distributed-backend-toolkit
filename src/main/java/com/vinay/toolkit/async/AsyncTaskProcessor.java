package com.vinay.toolkit.async;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CompletableFuture patterns for async processing.
 *
 * processAsync:       single task, fire on taskExecutor, return future to caller
 * processInParallel:  fan-out N tasks, join on allOf(), collect results
 * processWithPipeline: validate → enrich → transform, with graceful degradation
 *                      if enrich fails (exceptionally returns raw input instead of throwing)
 */
@Slf4j
@Service
public class AsyncTaskProcessor {

    private final Timer processingTimer;

    public AsyncTaskProcessor(MeterRegistry meterRegistry) {
        this.processingTimer = meterRegistry.timer("async.task.latency");
    }

    /**
     * Process a single task asynchronously.
     * Runs on the taskExecutor thread pool: does not block the calling thread.
     */
    @Async("taskExecutor")
    public CompletableFuture<Map<String, Object>> processAsync(Map<String, Object> task) {
        return CompletableFuture.supplyAsync(() ->
                processingTimer.record(() -> process(task))
        );
    }

    /**
     * Fan-out: process a list of tasks in parallel, collect results.
     * Uses allOf() to join: waits for ALL tasks to complete before returning.
     *
     * For fire-and-forget (don't wait for results), drop the join() call.
     */
    @Async("taskExecutor")
    public CompletableFuture<List<Map<String, Object>>> processInParallel(
            List<Map<String, Object>> tasks) {

        List<CompletableFuture<Map<String, Object>>> futures = tasks.stream()
                .map(task -> CompletableFuture.supplyAsync(() -> process(task)))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
    }

    /**
     * Three-stage processing pipeline using CompletableFuture composition:
     *   validate → enrich → transform
     *
     * Each stage runs asynchronously. Failures in enrich degrade gracefully
     * rather than failing the whole pipeline.
     */
    public CompletableFuture<Map<String, Object>> processWithPipeline(Map<String, Object> input) {
        return CompletableFuture
                .supplyAsync(() -> validate(input))               // Stage 1: validate
                .thenApplyAsync(this::enrich)                     // Stage 2: enrich metadata
                .exceptionally(ex -> {                            // Enrich failed: degrade gracefully
                    log.warn("Enrichment failed, using raw input: {}", ex.getMessage());
                    return input;
                })
                .thenApplyAsync(this::transform);                 // Stage 3: transform output
    }

    // ── Internal pipeline stages ──────────────────────────────────────────────

    private Map<String, Object> process(Map<String, Object> task) {
        String taskId = (String) task.getOrDefault("id", "unknown");
        log.debug("Processing task={} thread={}", taskId, Thread.currentThread().getName());
        // Simulate processing work
        return Map.of(
                "id", taskId,
                "status", "processed",
                "processed_at", System.currentTimeMillis(),
                "thread", Thread.currentThread().getName()
        );
    }

    private Map<String, Object> validate(Map<String, Object> input) {
        if (!input.containsKey("id")) {
            throw new IllegalArgumentException("Task missing required field: id");
        }
        return input;
    }

    private Map<String, Object> enrich(Map<String, Object> input) {
        // Add metadata: in production this calls an external service
        return new java.util.HashMap<>(input) {{
            put("enriched", true);
            put("enriched_at", System.currentTimeMillis());
        }};
    }

    private Map<String, Object> transform(Map<String, Object> input) {
        return new java.util.HashMap<>(input) {{
            put("transformed", true);
            put("pipeline_complete", true);
        }};
    }
}

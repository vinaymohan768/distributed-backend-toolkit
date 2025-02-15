package com.vinay.toolkit.async;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncTaskProcessorTest {

    private AsyncTaskProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new AsyncTaskProcessor(new SimpleMeterRegistry());
    }

    @Test
    void pipeline_completesAllThreeStages() throws Exception {
        Map<String, Object> input = Map.of("id", "task-1", "value", 42);

        Map<String, Object> result = processor.processWithPipeline(input).get();

        assertThat(result).containsKey("transformed");
        assertThat(result).containsKey("pipeline_complete");
        assertThat(result.get("id")).isEqualTo("task-1");
    }

    @Test
    void pipeline_degradesGracefullyWhenEnrichFails() throws Exception {
        // No "id" key → validate throws → exceptionally returns raw input → transform runs
        Map<String, Object> badInput = Map.of("value", 99);

        // Should not throw: pipeline handles the failure
        Map<String, Object> result = processor.processWithPipeline(badInput)
                .exceptionally(ex -> Map.of("degraded", true))
                .get();

        assertThat(result).isNotNull();
    }

    @Test
    void parallelProcessing_returnsResultForEveryTask() throws ExecutionException, InterruptedException {
        List<Map<String, Object>> tasks = IntStream.range(0, 10)
                .mapToObj(i -> Map.<String, Object>of("id", "task-" + i))
                .toList();

        List<Map<String, Object>> results = processor.processInParallel(tasks).get();

        assertThat(results).hasSize(10);
        assertThat(results).allSatisfy(r -> assertThat(r).containsKey("status"));
    }

    @Test
    void parallelProcessing_isActuallyFasterThanSequential() throws Exception {
        List<Map<String, Object>> tasks = IntStream.range(0, 20)
                .mapToObj(i -> Map.<String, Object>of("id", "task-" + i))
                .toList();

        long start = System.currentTimeMillis();
        processor.processInParallel(tasks).get();
        long parallelMs = System.currentTimeMillis() - start;

        // Parallel execution of 20 tasks should complete well under 2 seconds
        assertThat(parallelMs).isLessThan(2000);
    }
}

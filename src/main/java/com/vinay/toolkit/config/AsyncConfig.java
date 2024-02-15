package com.vinay.toolkit.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Async thread pool configuration.
 *
 * Sizing rationale for I/O-bound workloads:
 *   - core-pool-size = number of CPU cores * 2 (default: 8)
 *     Threads block on I/O (DB, Redis, Kafka), so we can run more than CPU cores
 *   - max-pool-size = 32 — hard ceiling to prevent thread explosion
 *   - queue-capacity = 1000 — bounded queue provides backpressure
 *     When queue is full, CallerRunsPolicy executes on the calling thread,
 *     effectively throttling the producer without dropping work.
 *
 * CallerRunsPolicy vs AbortPolicy:
 *   - AbortPolicy (default): throws RejectedExecutionException — work is lost
 *   - CallerRunsPolicy: calling thread executes the task — provides natural
 *     backpressure without data loss. Correct choice for batch pipelines.
 */
@Configuration
public class AsyncConfig {

    @Value("${toolkit.async.core-pool-size:8}")
    private int corePoolSize;

    @Value("${toolkit.async.max-pool-size:32}")
    private int maxPoolSize;

    @Value("${toolkit.async.queue-capacity:1000}")
    private int queueCapacity;

    @Value("${toolkit.async.thread-name-prefix:toolkit-async-}")
    private String threadNamePrefix;

    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(threadNamePrefix);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }
}

package com.vinay.toolkit.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Thread pool for @Async methods.
 *
 * core=8, max=32, queue=1000. CallerRunsPolicy on saturation: the calling
 * thread executes the task rather than dropping it, giving natural backpressure
 * without data loss (unlike AbortPolicy which throws and loses work).
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

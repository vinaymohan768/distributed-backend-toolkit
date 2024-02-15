package com.vinay.toolkit.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Partition-aware Kafka producer.
 *
 * Partition affinity strategy:
 *   All messages for the same entity (device, user, order) are routed to the
 *   same partition by using the entity ID as the Kafka message key.
 *   Kafka's default partitioner applies murmur2 hash on the key, deterministically
 *   mapping each key to one partition.
 *
 *   Why this matters:
 *   - Ordering guarantees: Kafka only guarantees order within a partition.
 *     Key-based routing ensures all events for device-X arrive in sequence.
 *   - Stateful consumers: sliding-window detectors, session state, and aggregations
 *     can be maintained in-memory per consumer thread without distributed locking.
 *
 * Async send with callback:
 *   KafkaTemplate.send() is non-blocking. The returned CompletableFuture
 *   resolves when the broker acknowledges (acks=all). Failures are logged
 *   with structured context for alerting.
 */
@Slf4j
@Component
public class PartitionAwareProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Counter sentCounter;
    private final Counter failedCounter;
    private final Timer sendTimer;

    public PartitionAwareProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.sentCounter = meterRegistry.counter("kafka.producer.sent");
        this.failedCounter = meterRegistry.counter("kafka.producer.failed");
        this.sendTimer = meterRegistry.timer("kafka.producer.send.latency");
    }

    /**
     * Send a single event keyed by entityId.
     * Returns CompletableFuture — caller decides whether to await or fire-and-forget.
     */
    public CompletableFuture<SendResult<String, String>> send(
            String topic, String entityId, Object payload) {
        return sendTimer.record(() -> {
            try {
                String json = objectMapper.writeValueAsString(payload);
                CompletableFuture<SendResult<String, String>> future =
                        kafkaTemplate.send(topic, entityId, json);

                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        failedCounter.increment();
                        log.error("Send failed | topic={} key={} error={}", topic, entityId, ex.getMessage());
                    } else {
                        sentCounter.increment();
                        log.debug("Sent | topic={} key={} partition={} offset={}",
                                topic, entityId,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

                return future;
            } catch (Exception e) {
                failedCounter.increment();
                log.error("Serialization failed | topic={} key={}", topic, entityId, e);
                return CompletableFuture.failedFuture(e);
            }
        });
    }

    /**
     * Batch send — fires all sends in parallel and waits for all to complete.
     * Leverages producer linger.ms + batch.size for automatic micro-batching.
     */
    public CompletableFuture<Void> sendBatch(String topic, List<Map.Entry<String, Object>> events) {
        List<CompletableFuture<SendResult<String, String>>> futures = events.stream()
                .map(e -> send(topic, e.getKey(), e.getValue()))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
}

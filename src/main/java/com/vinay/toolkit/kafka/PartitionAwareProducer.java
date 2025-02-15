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
 * Kafka producer that keys every message by entity ID (device_id, user_id, etc.)
 * so all events for one entity land on the same partition via murmur2 hash.
 *
 * This gives per-entity ordering guarantees and lets consumers maintain
 * in-memory state (sliding windows, sessions) without distributed locking.
 * Sends are async: callers get a CompletableFuture and decide whether to await.
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
     * Returns CompletableFuture: caller decides whether to await or fire-and-forget.
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
     * Batch send: fires all sends in parallel and waits for all to complete.
     * Leverages producer linger.ms + batch.size for automatic micro-batching.
     */
    public CompletableFuture<Void> sendBatch(String topic, List<Map.Entry<String, Object>> events) {
        List<CompletableFuture<SendResult<String, String>>> futures = events.stream()
                .map(e -> send(topic, e.getKey(), e.getValue()))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
}

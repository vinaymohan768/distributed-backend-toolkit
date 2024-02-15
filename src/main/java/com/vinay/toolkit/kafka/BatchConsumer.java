package com.vinay.toolkit.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Batch Kafka consumer with manual offset acknowledgment.
 *
 * Batch processing rationale:
 *   Processing records one-at-a-time has fixed per-call overhead for DB writes,
 *   Redis ops, and Kafka commit RPCs. Batch processing amortizes this overhead:
 *   - DB: batch INSERT vs N individual INSERTs (10–50x throughput improvement)
 *   - Kafka: one offset commit per batch vs one per record
 *   - Thread utilization: fewer context switches, better CPU cache usage
 *
 * Manual acknowledgment (AckMode.BATCH):
 *   ack.acknowledge() is called AFTER the batch is fully processed and persisted.
 *   If the consumer crashes mid-batch, Kafka redelivers the unacknowledged batch
 *   on restart — at-least-once delivery. Downstream processing must be idempotent
 *   (ON CONFLICT DO NOTHING in SQL, Redis SET NX, etc.).
 *
 * Concurrency:
 *   Spring Kafka creates one consumer thread per concurrency setting.
 *   With 6 partitions and concurrency=3, each thread handles 2 partitions.
 *   Scale to concurrency=6 for max throughput (one thread per partition).
 */
@Slf4j
@Component
public class BatchConsumer {

    private final ObjectMapper objectMapper;
    private final Counter processedCounter;
    private final Counter errorCounter;
    private final Timer batchTimer;

    public BatchConsumer(ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.processedCounter = meterRegistry.counter("kafka.consumer.processed");
        this.errorCounter = meterRegistry.counter("kafka.consumer.errors");
        this.batchTimer = meterRegistry.timer("kafka.consumer.batch.latency");
    }

    @KafkaListener(
            topics = "${toolkit.kafka.topics.events:device-events}",
            groupId = "${spring.kafka.consumer.group-id:toolkit-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records.isEmpty()) return;

        batchTimer.record(() -> {
            log.info("Received batch | size={} partitions={}",
                    records.size(), distinctPartitions(records));

            List<Map<String, Object>> parsed = new ArrayList<>(records.size());
            int errors = 0;

            for (ConsumerRecord<String, String> record : records) {
                try {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> payload = objectMapper.readValue(record.value(), Map.class);
                    payload.put("_partition", record.partition());
                    payload.put("_offset", record.offset());
                    payload.put("_key", record.key());
                    parsed.add(payload);
                } catch (Exception e) {
                    errors++;
                    log.warn("Deserialization failed | partition={} offset={} error={}",
                            record.partition(), record.offset(), e.getMessage());
                }
            }

            // Process the valid records
            processBatch(parsed);

            processedCounter.increment(parsed.size());
            errorCounter.increment(errors);

            // Commit offsets only after successful processing
            ack.acknowledge();

            log.info("Batch committed | processed={} errors={}", parsed.size(), errors);
        });
    }

    /**
     * Override this method in subclasses to implement domain-specific processing.
     * Default implementation logs the batch for demonstration.
     */
    protected void processBatch(List<Map<String, Object>> records) {
        log.debug("Processing {} records", records.size());
        // Subclasses: write to DB, call downstream service, update cache, etc.
    }

    private String distinctPartitions(List<ConsumerRecord<String, String>> records) {
        return records.stream()
                .map(r -> String.valueOf(r.partition()))
                .distinct()
                .sorted()
                .reduce((a, b) -> a + "," + b)
                .orElse("none");
    }
}

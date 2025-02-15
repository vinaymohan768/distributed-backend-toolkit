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
 * Batch Kafka consumer with manual AckMode.BATCH.
 *
 * Offsets are committed only after the full batch is processed and persisted :
 * at-least-once delivery. Downstream writes must be idempotent (ON CONFLICT DO NOTHING).
 * Deserialization errors skip the bad record without failing the whole batch.
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

package com.vinay.toolkit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration covering:
 *
 * Producer:
 *  - Idempotent producer (enable.idempotence=true) for exactly-once writes
 *  - acks=all ensures all ISR replicas confirm before returning
 *  - linger.ms=5 + batch.size=64KB for micro-batching throughput gains
 *  - LZ4 compression — best latency/ratio trade-off for telemetry payloads
 *
 * Consumer:
 *  - Batch listener: processes up to max-poll-records in one invocation
 *    reducing per-message overhead vs record-at-a-time processing
 *  - Manual AckMode.BATCH: commits offsets only after the full batch is
 *    processed and written downstream — prevents data loss on consumer crash
 *  - Concurrency=3: one thread per partition for parallel processing
 *    (set equal to partition count for maximum throughput)
 *
 * Topics created with 6 partitions — size for horizontal scaling:
 *  - 6 partitions allows up to 6 parallel consumer threads
 *  - Partition key = device_id ensures ordering per device
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${toolkit.kafka.concurrency:3}")
    private int concurrency;

    // ── Producer ──────────────────────────────────────────────────────────────

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ── Consumer ──────────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // manual commit
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * Batch listener factory: delivers List<ConsumerRecord> to the @KafkaListener method.
     * AckMode.BATCH commits the offset of the last record in the batch after the
     * listener method returns without throwing — equivalent to at-least-once delivery.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.getContainerProperties().setPollTimeout(1000);
        return factory;
    }

    // ── Topic declarations ────────────────────────────────────────────────────

    @Bean
    public NewTopic deviceEventsTopic() {
        return TopicBuilder.name("device-events")
                .partitions(6)
                .replicas(1)
                .config("retention.ms", "86400000")       // 24h retention
                .config("compression.type", "lz4")
                .build();
    }

    @Bean
    public NewTopic deviceResultsTopic() {
        return TopicBuilder.name("device-results")
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "604800000")      // 7d retention
                .build();
    }
}

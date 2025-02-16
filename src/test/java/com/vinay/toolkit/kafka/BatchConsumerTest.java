package com.vinay.toolkit.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class BatchConsumerTest {

    private BatchConsumer consumer;
    private Acknowledgment ack;

    @BeforeEach
    void setUp() {
        consumer = new BatchConsumer(new ObjectMapper(), new SimpleMeterRegistry());
        ack = mock(Acknowledgment.class);
    }

    @Test
    void consume_acknowledgesOffsetAfterProcessing() {
        List<ConsumerRecord<String, String>> records = List.of(
                record("device-1", """{"device_id":"device-1","value":42}"""),
                record("device-2", """{"device_id":"device-2","value":99}""")
        );

        consumer.consume(records, ack);

        // Offset must be committed after successful processing
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void consume_skipsInvalidJsonWithoutFailingBatch() {
        List<ConsumerRecord<String, String>> records = List.of(
                record("device-1", """{"device_id":"device-1"}"""),
                record("device-2", "NOT_VALID_JSON"),   // bad record
                record("device-3", """{"device_id":"device-3"}""")
        );

        // Should not throw — bad record is skipped, rest of batch proceeds
        consumer.consume(records, ack);

        // Offset still committed for the valid records
        verify(ack, times(1)).acknowledge();
    }

    @Test
    void consume_emptyBatchIsNoOp() {
        consumer.consume(List.of(), ack);
        verifyNoInteractions(ack);
    }

    @Test
    void consume_attachesPartitionAndOffsetMetadata() {
        AtomicBoolean metadataPresent = new AtomicBoolean(false);

        BatchConsumer consumerWithHook = new BatchConsumer(new ObjectMapper(), new SimpleMeterRegistry()) {
            @Override
            protected void processBatch(java.util.List<java.util.Map<String, Object>> records) {
                metadataPresent.set(
                        records.stream().allMatch(r -> r.containsKey("_partition") && r.containsKey("_offset"))
                );
            }
        };

        consumerWithHook.consume(
                List.of(record("device-1", """{"id":"1"}""")),
                ack
        );

        assertThat(metadataPresent.get()).isTrue();
    }

    private ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>("device-events", 0, 0L, key, value);
    }
}

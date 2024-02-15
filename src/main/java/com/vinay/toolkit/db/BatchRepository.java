package com.vinay.toolkit.db;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * High-throughput PostgreSQL repository using batch inserts and optimized queries.
 *
 * Key patterns demonstrated:
 *
 * 1. JdbcTemplate batch insert (batchUpdate)
 *    Sends N rows in a single prepared statement execution.
 *    At batch-size=500, reduces DB round-trips by ~500x vs individual inserts.
 *    HikariCP connection pooling ensures connections are reused, not recreated.
 *
 * 2. ON CONFLICT DO NOTHING
 *    Idempotent inserts — safe to re-insert if Kafka redelivers a batch.
 *    Required for at-least-once delivery semantics without duplicate rows.
 *
 * 3. Partition pruning via timestamp filter
 *    All queries include event_timestamp in the WHERE clause.
 *    PostgreSQL uses partition pruning to scan only the relevant monthly partition,
 *    keeping query time flat as total data grows (avoids full table scans).
 *
 * 4. Composite index exploitation
 *    Queries filter on (device_id, event_timestamp DESC) — matching the composite
 *    B-tree index defined in schema.sql. PostgreSQL uses index-only scans for
 *    this access pattern, returning results without touching the heap.
 *
 * 5. Read/write split via @Transactional(readOnly=true)
 *    Marks read queries as read-only, allowing Spring/HikariCP to route them
 *    to a read replica if configured (standard production pattern for scaling reads).
 */
@Slf4j
@Repository
public class BatchRepository {

    private final JdbcTemplate jdbc;
    private final Timer batchInsertTimer;
    private final Timer queryTimer;

    @Value("${toolkit.db.batch-insert-size:500}")
    private int batchSize;

    public BatchRepository(JdbcTemplate jdbc, MeterRegistry meterRegistry) {
        this.jdbc = jdbc;
        this.batchInsertTimer = meterRegistry.timer("db.batch.insert.latency");
        this.queryTimer        = meterRegistry.timer("db.query.latency");
    }

    /**
     * Batch insert events into the partitioned telemetry table.
     * Returns the number of rows inserted.
     */
    @Transactional
    public int insertEventsBatch(List<Map<String, Object>> events) {
        if (events.isEmpty()) return 0;

        String sql = """
                INSERT INTO events (
                    device_id, event_type, payload, event_timestamp
                ) VALUES (?, ?, ?::jsonb, ?)
                ON CONFLICT (device_id, event_timestamp) DO NOTHING
                """;

        int[] results = batchInsertTimer.record(() ->
                jdbc.batchUpdate(sql, events, batchSize, (ps, event) -> {
                    ps.setString(1, (String) event.get("device_id"));
                    ps.setString(2, (String) event.getOrDefault("event_type", "unknown"));
                    ps.setString(3, (String) event.getOrDefault("payload", "{}"));
                    Object ts = event.get("event_timestamp");
                    ps.setTimestamp(4, ts != null
                            ? Timestamp.from(Instant.parse(ts.toString()))
                            : Timestamp.from(Instant.now()));
                })
        );

        int total = 0;
        for (int[] batch : results) {
            for (int r : batch) total += Math.max(r, 0);
        }

        log.debug("Batch insert | rows_attempted={} rows_inserted={}", events.size(), total);
        return total;
    }

    /**
     * Retrieve recent events for a device — exploits composite index (device_id, event_timestamp DESC).
     * readOnly=true signals the connection pool to use a read replica if configured.
     */
    @Transactional(readOnly = true)
    public List<Map<String, Object>> getRecentEvents(String deviceId, int limit, int sinceMinutes) {
        String sql = """
                SELECT device_id, event_type, payload, event_timestamp
                FROM events
                WHERE device_id = ?
                  AND event_timestamp >= NOW() - INTERVAL '%d minutes'
                ORDER BY event_timestamp DESC
                LIMIT ?
                """.formatted(sinceMinutes);

        return queryTimer.record(() ->
                jdbc.queryForList(sql, deviceId, limit)
        );
    }

    /**
     * Aggregate stats per device over a time window.
     * Range filter on event_timestamp enables partition pruning.
     */
    @Transactional(readOnly = true)
    public List<Map<String, Object>> getDeviceStats(int windowMinutes) {
        String sql = """
                SELECT
                    device_id,
                    COUNT(*)                                                AS total_events,
                    SUM(CASE WHEN event_type = 'critical' THEN 1 ELSE 0 END) AS critical_count,
                    MAX(event_timestamp)                                    AS last_seen
                FROM events
                WHERE event_timestamp >= NOW() - INTERVAL '%d minutes'
                GROUP BY device_id
                ORDER BY total_events DESC
                LIMIT 100
                """.formatted(windowMinutes);

        return queryTimer.record(() -> jdbc.queryForList(sql));
    }
}

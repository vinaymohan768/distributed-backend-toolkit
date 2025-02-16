package com.vinay.toolkit.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Two-tier cache: L1 Caffeine (in-process, &lt;1μs) → L2 Redis (shared, ~1ms).
 *
 * Read:  L1 hit → return. Miss → check L2 → backfill L1 → return.
 *        Full miss → load from source → populate both tiers.
 * Write: invalidate L1 + L2. Write-through is intentionally skipped —
 *        it causes coherency bugs across JVM instances. Invalidate instead.
 */
@Slf4j
@Service
public class TieredCacheService {

    private final CacheManager caffeineCacheManager;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    private final Counter l1HitCounter;
    private final Counter l2HitCounter;
    private final Counter missCounter;

    @Value("${toolkit.cache.l2-ttl-seconds:300}")
    private long l2TtlSeconds;

    private static final String L1_CACHE_NAME = "telemetry-l1";

    public TieredCacheService(
            CacheManager caffeineCacheManager,
            RedisTemplate<String, String> redisTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this.caffeineCacheManager = caffeineCacheManager;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.l1HitCounter = meterRegistry.counter("cache.l1.hits");
        this.l2HitCounter = meterRegistry.counter("cache.l2.hits");
        this.missCounter   = meterRegistry.counter("cache.misses");
    }

    /** Read from L1 → L2 → loader. Populates tiers on miss. */
    public <T> Optional<T> get(String key, TypeReference<T> type, Callable<T> loader) {
        // L1 — Caffeine
        Cache l1 = caffeineCacheManager.getCache(L1_CACHE_NAME);
        if (l1 != null) {
            Cache.ValueWrapper wrapped = l1.get(key);
            if (wrapped != null) {
                l1HitCounter.increment();
                log.debug("L1 hit | key={}", key);
                @SuppressWarnings("unchecked")
                T val = (T) wrapped.get();
                return Optional.ofNullable(val);
            }
        }

        // L2 — Redis
        String redisValue = redisTemplate.opsForValue().get(key);
        if (redisValue != null) {
            l2HitCounter.increment();
            log.debug("L2 hit | key={}", key);
            try {
                T val = objectMapper.readValue(redisValue, type);
                // Backfill L1
                if (l1 != null) l1.put(key, val);
                return Optional.of(val);
            } catch (Exception e) {
                log.warn("L2 deserialization failed for key={}", key, e);
            }
        }

        // Full miss — load from source
        missCounter.increment();
        log.debug("Cache miss | key={}", key);
        try {
            T val = loader.call();
            if (val != null) {
                put(key, val);
            }
            return Optional.ofNullable(val);
        } catch (Exception e) {
            log.error("Cache loader failed for key={}", key, e);
            return Optional.empty();
        }
    }

    /** Write to L2 first (shared state), then backfill L1. */
    public <T> void put(String key, T value) {
        try {
            String json = objectMapper.writeValueAsString(value);
            // L2 first — shared state is authoritative
            redisTemplate.opsForValue().set(key, json, Duration.ofSeconds(l2TtlSeconds));
            // L1 — local fast path
            Cache l1 = caffeineCacheManager.getCache(L1_CACHE_NAME);
            if (l1 != null) l1.put(key, value);
        } catch (Exception e) {
            log.error("Cache put failed for key={}", key, e);
        }
    }

    /** Remove a key from both tiers. Call after any write to the source. */
    public void evict(String key) {
        redisTemplate.delete(key);
        Cache l1 = caffeineCacheManager.getCache(L1_CACHE_NAME);
        if (l1 != null) l1.evict(key);
        log.debug("Evicted | key={}", key);
    }

    /** Atomic Redis increment — O(1), no lock needed. Sets TTL on first increment. */
    public long increment(String key, long delta, Duration ttl) {
        Long result = redisTemplate.opsForValue().increment(key, delta);
        if (result != null && result == delta) {
            // First increment — set TTL (avoids counter living forever)
            redisTemplate.expire(key, ttl);
        }
        return result != null ? result : 0L;
    }
}

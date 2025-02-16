package com.vinay.toolkit.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TieredCacheServiceTest {

    private TieredCacheService cache;
    private RedisTemplate<String, String> redis;
    private ValueOperations<String, String> valueOps;
    private CacheManager caffeine;
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(RedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(valueOps);

        CaffeineCacheManager cm = new CaffeineCacheManager("telemetry-l1");
        cm.setCacheSpecification("maximumSize=1000,expireAfterWrite=60s");
        caffeine = cm;

        cache = new TieredCacheService(caffeine, redis, new ObjectMapper(), new SimpleMeterRegistry());
    }

    @Test
    void get_returnsL1HitWithoutTouchingRedis() throws Exception {
        // Pre-populate L1
        cache.put("key-1", Map.of("data", "cached"));

        AtomicInteger loaderCalls = new AtomicInteger(0);
        Optional<Map<String, Object>> result = cache.get("key-1", MAP_TYPE, () -> {
            loaderCalls.incrementAndGet();
            return Map.of("data", "from-db");
        });

        assertThat(result).isPresent();
        assertThat(loaderCalls.get()).isZero();       // loader never called
        verify(valueOps, never()).get(anyString());   // Redis never touched
    }

    @Test
    void get_fallsThoughToLoaderOnFullMiss() throws Exception {
        when(valueOps.get("missing-key")).thenReturn(null);

        AtomicInteger loaderCalls = new AtomicInteger(0);
        Optional<Map<String, Object>> result = cache.get("missing-key", MAP_TYPE, () -> {
            loaderCalls.incrementAndGet();
            return Map.of("data", "from-db");
        });

        assertThat(result).isPresent();
        assertThat(result.get()).containsEntry("data", "from-db");
        assertThat(loaderCalls.get()).isEqualTo(1);
    }

    @Test
    void evict_removesKeyFromBothTiers() {
        cache.put("key-evict", Map.of("v", 1));

        cache.evict("key-evict");

        // L1 should be gone
        Optional<Map<String, Object>> result = cache.get("key-evict", MAP_TYPE, () -> null);
        verify(redis).delete("key-evict");
    }

    @Test
    void increment_setsTtlOnFirstCall() {
        when(valueOps.increment("counter", 1L)).thenReturn(1L);  // first increment

        cache.increment("counter", 1L, Duration.ofMinutes(1));

        verify(redis).expire(eq("counter"), any(Duration.class));
    }

    @Test
    void increment_doesNotResetTtlOnSubsequentCalls() {
        when(valueOps.increment("counter", 1L)).thenReturn(5L);  // not first

        cache.increment("counter", 1L, Duration.ofMinutes(1));

        verify(redis, never()).expire(any(), any());
    }
}

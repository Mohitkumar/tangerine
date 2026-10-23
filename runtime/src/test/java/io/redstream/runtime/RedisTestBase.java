package io.redstream.runtime;

import io.redstream.redis.LettuceRedisOps;
import io.redstream.redis.RedisOps;
import io.redstream.testkit.TestRedis;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

/**
 * One Redis per test class (a container, or {@code REDSTREAM_REDIS_URI}). Tests share it, so
 * each test uses its own job name from {@link #newJob}.
 *
 * <p>Get the objects under test from {@link Impl} <b>before</b> any {@code assertThrows}: a
 * "not wired yet" skip inside {@code assertThrows} would be caught and counted as a pass.
 */
@Tag("integration")
public abstract class RedisTestBase {

    protected static TestRedis redis;
    protected static RedisOps ops;

    @BeforeAll
    static void startRedis() {
        redis = TestRedis.start();
        ops = LettuceRedisOps.standalone(redis.uri());
    }

    @AfterAll
    static void stopRedis() {
        if (ops != null) {
            ops.close();
        }
        if (redis != null) {
            redis.close();
        }
    }

    /** A unique, valid job name. */
    protected static String newJob(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}

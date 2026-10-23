package io.redstream.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * @param batchSize    max records per read, and therefore per atomic commit
 * @param pollInterval sleep when an input stream has nothing new (reads never block; see LettuceRedisOps)
 * @param leaseTtl     how long a partition stays owned without renewal; failover takes about this long
 */
public record WorkerConfig(String redisUri, boolean cluster, String workerId, int batchSize, Duration pollInterval, Duration leaseTtl) {

    public WorkerConfig {
        Objects.requireNonNull(redisUri, "redisUri");
        Objects.requireNonNull(workerId, "workerId");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        if (leaseTtl.toMillis() < 300) {
            throw new IllegalArgumentException("leaseTtl must be >= 300ms");
        }
    }

    public static WorkerConfig defaults(String redisUri) {
        return new WorkerConfig(redisUri, false, "worker-" + UUID.randomUUID().toString().substring(0, 8),
                500, Duration.ofMillis(50), Duration.ofSeconds(10));
    }

    /** Reads REDSTREAM_REDIS_URI, REDSTREAM_CLUSTER, REDSTREAM_WORKER_ID, REDSTREAM_BATCH_SIZE. */
    public static WorkerConfig fromEnv() {
        var c = defaults(env("REDSTREAM_REDIS_URI", "redis://localhost:6379"));
        return new WorkerConfig(c.redisUri(),
                Boolean.parseBoolean(env("REDSTREAM_CLUSTER", "false")),
                env("REDSTREAM_WORKER_ID", c.workerId()),
                Integer.parseInt(env("REDSTREAM_BATCH_SIZE", Integer.toString(c.batchSize()))),
                c.pollInterval(), c.leaseTtl());
    }

    public WorkerConfig withWorkerId(String id) {
        return new WorkerConfig(redisUri, cluster, id, batchSize, pollInterval, leaseTtl);
    }

    public WorkerConfig withBatchSize(int size) {
        return new WorkerConfig(redisUri, cluster, workerId, size, pollInterval, leaseTtl);
    }

    public WorkerConfig withLeaseTtl(Duration ttl) {
        return new WorkerConfig(redisUri, cluster, workerId, batchSize, pollInterval, ttl);
    }

    private static String env(String name, String dflt) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? dflt : v;
    }
}

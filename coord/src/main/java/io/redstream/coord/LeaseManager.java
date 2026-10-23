package io.redstream.coord;

import io.redstream.redis.LuaScript;
import io.redstream.redis.PartitionKeys;
import io.redstream.redis.RedisOps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;

/**
 * Partition ownership through TTL leases stored next to the partition's data. Acquiring a
 * free lease increments the partition epoch; every commit checks it. A worker that stalls
 * (GC pause, network partition) past its TTL therefore cannot write after someone else takes
 * over: its next commit fails with FENCED.
 */
public final class LeaseManager {

    private static final LuaScript LEASE = LuaScript.load(LeaseManager.class, "/io/redstream/coord/lua/lease.lua");
    private static final LuaScript RELEASE = LuaScript.load(LeaseManager.class, "/io/redstream/coord/lua/release.lua");

    private final RedisOps ops;
    private final String workerId;
    private final Duration ttl;

    public LeaseManager(RedisOps ops, String workerId, Duration ttl) {
        this.ops = ops;
        this.workerId = workerId;
        this.ttl = ttl;
    }

    /** Acquires or renews. Returns the epoch to write with, or empty if another worker holds it. */
    public OptionalLong acquireOrRenew(PartitionKeys keys) {
        long epoch = ops.evalLong(LEASE, List.of(keys.lease(), keys.meta()),
                List.of(utf8(workerId), utf8(Long.toString(ttl.toMillis()))));
        return epoch < 0 ? OptionalLong.empty() : OptionalLong.of(epoch);
    }

    /** Releases if still held. Call only after the partition's task has fully stopped. */
    public boolean release(PartitionKeys keys) {
        return ops.evalLong(RELEASE, List.of(keys.lease()), List.of(utf8(workerId))) == 1;
    }

    public String workerId() {
        return workerId;
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

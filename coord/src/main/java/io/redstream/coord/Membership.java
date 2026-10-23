package io.redstream.coord;

import io.redstream.redis.KeySpace;
import io.redstream.redis.LuaScript;
import io.redstream.redis.RedisOps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/** Tracks live workers with heartbeats in a ZSET. There is no JobManager: workers coordinate through Redis. */
public final class Membership {

    private static final LuaScript HEARTBEAT = LuaScript.load(Membership.class, "/io/redstream/coord/lua/heartbeat.lua");

    private final RedisOps ops;
    private final KeySpace keySpace;
    private final String workerId;
    private final Duration ttl;

    public Membership(RedisOps ops, KeySpace keySpace, String workerId, Duration ttl) {
        this.ops = ops;
        this.keySpace = keySpace;
        this.workerId = workerId;
        this.ttl = ttl;
    }

    /** Heartbeats and returns live worker ids, sorted, including this one. */
    public List<String> heartbeat() {
        return ops.evalList(HEARTBEAT, List.of(keySpace.workers()),
                        List.of(workerId.getBytes(StandardCharsets.UTF_8), Long.toString(ttl.toMillis()).getBytes(StandardCharsets.UTF_8)))
                .stream()
                .map(b -> new String(b, StandardCharsets.UTF_8))
                .sorted()
                .toList();
    }
}

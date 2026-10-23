package io.redstream.runtime.fault;

import io.redstream.redis.PartitionKeys;

/** Called by tasks at each {@link CrashPoint}. Production uses {@link #NONE}. */
@FunctionalInterface
public interface FaultHooks {

    FaultHooks NONE = (point, keys) -> {};

    void at(CrashPoint point, PartitionKeys keys);
}

package io.redstream.runtime.operator;

import io.redstream.coord.PartitionId;
import io.redstream.runtime.state.KeyedStateBackend;

/**
 * What the operators of one task can see, like the parts of Flink's {@code StreamTask} and
 * {@code RuntimeContext} that operators use. {@code StreamTask} implements it (plan step 1).
 */
public interface TaskContext {

    String jobName();

    /** The partition this task runs: a source partition or a key group. */
    PartitionId partition();

    /** Number of key groups; a shuffle uses it to pick the target key group. */
    int maxParallelism();

    /** Keyed state of this key group; {@code null} in a source stage. */
    KeyedStateBackend keyedState();

    long currentWatermark();

    /**
     * Sends a record out of the task: to a downstream key group's input stream (a shuffle) or to
     * a sink stream. Hint: buffer and send with one pipelined round trip per batch rather than
     * one call per record.
     */
    void emit(String stream, byte[] payload, long timestamp);
}

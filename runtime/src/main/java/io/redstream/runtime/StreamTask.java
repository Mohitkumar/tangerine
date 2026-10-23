package io.redstream.runtime;

import io.redstream.plan.JobGraph;
import io.redstream.plan.Stage;
import io.redstream.redis.KeySpace;
import io.redstream.redis.PartitionKeys;
import io.redstream.redis.RedisOps;
import io.redstream.runtime.fault.FaultHooks;

/**
 * Runs one partition (a source partition or a key group) while this worker holds its lease,
 * like Flink's {@code StreamTask}. {@link Worker} creates one per owned partition and runs it on
 * a virtual thread.
 *
 * <p>Plan step 1 writes the loop: read a batch from the input stream, run it through the
 * operator chain, send the outputs. Later steps add barriers, snapshots and acknowledgements.
 */
public final class StreamTask implements Runnable {

    private final JobGraph graph;
    private final Stage stage;
    private final PartitionKeys keys;
    private final long epoch;
    private final RedisOps ops;
    private final KeySpace keySpace;
    private final WorkerConfig config;
    private final FaultHooks hooks;
    private volatile boolean running = true;

    public StreamTask(JobGraph graph, Stage stage, int partition, long epoch, RedisOps ops, KeySpace keySpace,
                      WorkerConfig config, FaultHooks hooks) {
        this.graph = graph;
        this.stage = stage;
        this.keys = keySpace.partition(stage.id(), partition);
        this.epoch = epoch;
        this.ops = ops;
        this.keySpace = keySpace;
        this.config = config;
        this.hooks = hooks;
    }

    /** Asks the loop to exit after the current batch. */
    public void stop() {
        running = false;
    }

    public PartitionKeys keys() {
        return keys;
    }

    @Override
    public void run() {
        // TODO plan step 1: restore, build the operator chain, then loop while `running`.
        throw new UnsupportedOperationException("StreamTask is not implemented yet: see PLAN.md step 1");
    }
}

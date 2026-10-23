package io.redstream.runtime;

import io.redstream.api.CheckpointConfig;
import io.redstream.api.StreamEnvironment;
import io.redstream.coord.LeaderElection;
import io.redstream.plan.JobGraph;
import io.redstream.redis.RedisOps;
import io.redstream.runtime.checkpoint.CheckpointBarrierHandler;
import io.redstream.runtime.checkpoint.CheckpointCoordinator;
import io.redstream.runtime.checkpoint.CheckpointStorage;
import io.redstream.runtime.checkpoint.JobControl;
import io.redstream.runtime.sink.TwoPhaseCommitSink;
import io.redstream.runtime.state.StateBackend;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Assumptions;

/**
 * The one file to edit so the plan's tests can reach your code. Each method stands for one
 * thing PLAN.md asks you to build. Until you replace its body, every test that needs it is
 * reported as <b>skipped</b>, with the step to do next, so the build stays green while you work.
 *
 * <p>When you finish a piece, replace the {@code notYet(...)} line with a call to your class,
 * then run that step's tests (the commands are in PLAN.md).
 */
public final class Impl {

    private Impl() {}

    // ---- Step 1: running a job ----------------------------------------------------------

    /** Your {@code HashMapStateBackend}. */
    public static StateBackend heapStateBackend() {
        return notYet(1, "return new HashMapStateBackend();");
    }

    // ---- Step 2: snapshots and checkpoint storage ---------------------------------------

    /** Your Redis {@code CheckpointStorage} for job {@code jobName}. */
    public static CheckpointStorage redisCheckpointStorage(RedisOps ops, String jobName) {
        return notYet(2, "return new RedisCheckpointStorage(ops, new KeySpace(jobName));");
    }

    // ---- Step 3: leader and coordinator -------------------------------------------------

    public static LeaderElection leaderElection(RedisOps ops, String jobName, String workerId, Duration ttl) {
        return notYet(3, "return new RedisLeaderElection(ops, new KeySpace(jobName), workerId, ttl);");
    }

    public static JobControl jobControl(RedisOps ops, String jobName) {
        return notYet(3, "return new RedisJobControl(ops, new KeySpace(jobName));");
    }

    /**
     * A coordinator for one leadership term.
     *
     * @param clockMillis the time source for timeouts; tests pass a fake clock
     */
    public static CheckpointCoordinator checkpointCoordinator(JobGraph graph, JobControl control, CheckpointStorage storage,
                                                              CheckpointConfig config, long leaderEpoch, LongSupplier clockMillis) {
        return notYet(3, "return new DefaultCheckpointCoordinator(graph, control, storage, config, leaderEpoch, clockMillis);");
    }

    // ---- Step 4: checkpointing a running job --------------------------------------------

    /** Turns checkpointing on for jobs built from {@code env}. */
    public static void enableCheckpointing(StreamEnvironment env, CheckpointConfig config) {
        notYet(4, "env.enableCheckpointing(config);");
    }

    // ---- Step 5: barriers across keyBy --------------------------------------------------

    public static CheckpointBarrierHandler barrierHandler(Set<String> producers) {
        return notYet(5, "return new AligningBarrierHandler(producers);");
    }

    // ---- Step 6: RocksDB ----------------------------------------------------------------

    /** Your {@code EmbeddedRocksDBStateBackend}. */
    public static StateBackend rocksDbStateBackend() {
        return notYet(6, "return new EmbeddedRocksDBStateBackend();");
    }

    /** {@code base} switched to the RocksDB backend, keeping local files under {@code localStateDir}. */
    public static WorkerConfig withRocksDb(WorkerConfig base, Path localStateDir) {
        return notYet(6, "return base.withStateBackend(\"rocksdb\").withLocalStateDir(localStateDir);");
    }

    // ---- Step 7: exactly-once sink ------------------------------------------------------

    /**
     * Your two-phase-commit sink writing strings to {@code sinkStream}. Committed entries must
     * carry the value in field {@code v} ({@code MetaFields.F_VALUE}), like every sink entry.
     */
    public static TwoPhaseCommitSink<String, ?> redisStreamSink(RedisOps ops, String jobName, String sinkStream) {
        return notYet(7, "return new RedisTwoPhaseCommitSink<>(ops, new KeySpace(jobName), sinkStream, Serdes.string());");
    }

    /** {@code true} once jobs with checkpointing write to sinks through your two-phase-commit sink. */
    public static boolean sinksAreTransactional() {
        return notYet(7, "return true;");
    }

    // ---- Step 8: savepoints -------------------------------------------------------------

    /** Asks the running job's leader to take a savepoint. */
    public static void triggerSavepoint(RedisOps ops, String jobName) {
        notYet(8, "Savepoints.trigger(ops, new KeySpace(jobName));");
    }

    private static <T> T notYet(int step, String hint) {
        return Assumptions.abort("PLAN.md step " + step + " not wired yet. In Impl.java: " + hint);
    }
}

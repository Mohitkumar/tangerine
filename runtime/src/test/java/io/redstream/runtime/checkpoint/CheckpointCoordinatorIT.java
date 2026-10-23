package io.redstream.runtime.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.api.CheckpointConfig;
import io.redstream.api.StreamEnvironment;
import io.redstream.api.connector.RedisStreamSink;
import io.redstream.api.connector.RedisStreamSource;
import io.redstream.api.functions.Collector;
import io.redstream.api.functions.KeyedProcessFunction;
import io.redstream.api.serde.Serdes;
import io.redstream.api.time.WatermarkStrategy;
import io.redstream.coord.Assignment;
import io.redstream.coord.PartitionId;
import io.redstream.plan.JobGraph;
import io.redstream.plan.LogicalPlanner;
import io.redstream.runtime.Impl;
import io.redstream.runtime.RedisTestBase;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The coordinator's state machine, driven by hand: fake tasks write acknowledgements straight
 * into {@link JobControl}, and a fake clock controls timeouts (plan step 3).
 *
 * <p>The job has 2 source partitions (stage 0) and 4 key groups (stage 1): 6 partitions in all.
 */
class CheckpointCoordinatorIT extends RedisTestBase {

    private static final CheckpointConfig CONFIG = new CheckpointConfig(Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ZERO);

    static final class Forward extends KeyedProcessFunction<String, String, String> {
        @Override
        public void processElement(String value, Context<String> ctx, Collector<String> out) {
            out.collect(value);
        }
    }

    private final AtomicLong clock = new AtomicLong(10_000);
    private JobGraph graph;
    private List<PartitionId> partitions;
    private JobControl control;
    private CheckpointStorage storage;
    private long leaderEpoch;
    private CheckpointCoordinator coordinator;

    @BeforeEach
    void setUp() {
        String job = newJob("coord");
        var env = StreamEnvironment.create().maxParallelism(4);
        env.fromSource(RedisStreamSource.of(Serdes.string(), job + ":in:0", job + ":in:1"), WatermarkStrategy.noWatermarks(), "lines")
                .keyBy(s -> s, Serdes.string())
                .process("forward", Forward::new, Serdes.string())
                .sinkTo(RedisStreamSink.of(job + ":out", Serdes.string()));
        graph = LogicalPlanner.plan(env.build(job));
        partitions = Assignment.allPartitions(graph.stages().stream().mapToInt(s -> s.partitionCount(graph.maxParallelism())).toArray());

        control = Impl.jobControl(ops, job);
        storage = Impl.redisCheckpointStorage(ops, job);
        leaderEpoch = Impl.leaderElection(ops, job, "leader", Duration.ofSeconds(30)).acquireOrRenew().orElseThrow();
        coordinator = Impl.checkpointCoordinator(graph, control, storage, CONFIG, leaderEpoch, clock::get);
    }

    private TaskSnapshot ack(PartitionId p, long checkpointId) {
        return new TaskSnapshot(p, checkpointId, p.stage() == 0 ? "1-0" : null, 0, null, Map.of());
    }

    private void ackAll(long checkpointId) {
        partitions.forEach(p -> control.acknowledge(control.currentAttempt(), ack(p, checkpointId)));
    }

    @Test
    void step3_jobHasSixPartitions() {
        assertEquals(6, partitions.size(), "test setup");
    }

    @Test
    void step3_triggerPublishesTheRequest() {
        long id = coordinator.triggerCheckpoint(false);
        assertEquals(1, id, "a fresh job's first checkpoint is 1");
        assertEquals(new CheckpointBarrier(1, 10_000), control.requestedCheckpoint().orElseThrow());
    }

    @Test
    void step3_completesOnlyOnceEveryPartitionHasAcknowledged() {
        long id = coordinator.triggerCheckpoint(false);
        partitions.subList(0, 5).forEach(p -> control.acknowledge(0, ack(p, id)));
        coordinator.poll();
        assertTrue(storage.latestCompleted().isEmpty(), "5 of 6 acknowledged");

        control.acknowledge(0, ack(partitions.get(5), id));
        clock.set(10_400);
        coordinator.poll();

        CheckpointMetadata done = storage.latestCompleted().orElseThrow();
        assertEquals(id, done.checkpointId());
        assertEquals(6, done.tasks().size());
        assertEquals(10_000, done.triggeredAtMillis());
        assertEquals(10_400, done.completedAtMillis());
        assertFalse(done.savepoint());
    }

    @Test
    void step3_pollingAgainChangesNothing() {
        long id = coordinator.triggerCheckpoint(false);
        ackAll(id);
        coordinator.poll();
        coordinator.poll();
        assertEquals(id, storage.latestCompleted().orElseThrow().checkpointId());
    }

    @Test
    void step3_nextCheckpointGetsTheNextId() {
        long first = coordinator.triggerCheckpoint(false);
        ackAll(first);
        coordinator.poll();
        assertEquals(first + 1, coordinator.triggerCheckpoint(false));
    }

    @Test
    void step3_aDeclineAbortsTheCheckpoint() {
        long id = coordinator.triggerCheckpoint(false);
        partitions.subList(0, 5).forEach(p -> control.acknowledge(0, ack(p, id)));
        control.decline(0, id, partitions.get(5), "test decline");
        coordinator.poll();

        control.acknowledge(0, ack(partitions.get(5), id)); // too late: already aborted
        coordinator.poll();
        assertTrue(storage.latestCompleted().isEmpty());
        assertEquals(id + 1, coordinator.triggerCheckpoint(false), "the job carries on with a new checkpoint");
    }

    @Test
    void step3_aTimeoutAbortsTheCheckpoint() {
        long id = coordinator.triggerCheckpoint(false);
        clock.addAndGet(CONFIG.timeout().toMillis() + 1);
        coordinator.poll();

        ackAll(id);
        coordinator.poll();
        assertTrue(storage.latestCompleted().isEmpty(), "acks after the timeout do not complete it");
        assertEquals(id + 1, coordinator.triggerCheckpoint(false));
    }

    @Test
    void step3_acknowledgementsFromAnOldAttemptDoNotCount() {
        long id = coordinator.triggerCheckpoint(false);
        control.requestRestart(leaderEpoch, "test restart");
        partitions.forEach(p -> control.acknowledge(0, ack(p, id)));
        coordinator.poll();
        assertTrue(storage.latestCompleted().isEmpty());
    }

    @Test
    void step3_aSavepointIsMarked() {
        long id = coordinator.triggerCheckpoint(true);
        ackAll(id);
        coordinator.poll();
        assertTrue(storage.latestCompleted().orElseThrow().savepoint());
    }

    @Test
    void step3_aNewLeaderContinuesTheIds() {
        long first = coordinator.triggerCheckpoint(false);
        ackAll(first);
        coordinator.poll();
        coordinator.close();

        var next = Impl.checkpointCoordinator(graph, control, storage, CONFIG, leaderEpoch, clock::get);
        assertTrue(next.triggerCheckpoint(false) > first, "ids never repeat, even across leaders");
        next.close();
    }
}

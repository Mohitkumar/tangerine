package io.redstream.runtime.checkpoint;

import static io.redstream.runtime.checkpoint.Snapshots.keyed;
import static io.redstream.runtime.checkpoint.Snapshots.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.coord.PartitionId;
import io.redstream.redis.FencedException;
import io.redstream.runtime.Impl;
import io.redstream.runtime.RedisTestBase;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The messages between coordinator and tasks, kept in Redis (plan step 3). */
class JobControlIT extends RedisTestBase {

    @Test
    void step3_noCheckpointRequestedAtFirst() {
        var control = Impl.jobControl(ops, newJob("control"));
        assertEquals(Optional.empty(), control.requestedCheckpoint());
        assertEquals(0, control.currentAttempt());
    }

    @Test
    void step3_requestedCheckpointIsTheLatestRequest() {
        var control = Impl.jobControl(ops, newJob("control"));
        control.requestCheckpoint(1, new CheckpointBarrier(1, 1_000));
        assertEquals(Optional.of(new CheckpointBarrier(1, 1_000)), control.requestedCheckpoint());
        control.requestCheckpoint(1, new CheckpointBarrier(2, 2_000));
        assertEquals(Optional.of(new CheckpointBarrier(2, 2_000)), control.requestedCheckpoint());
    }

    @Test
    void step3_restartBumpsTheAttempt() {
        var control = Impl.jobControl(ops, newJob("control"));
        assertEquals(1, control.requestRestart(1, "task s1p3 failed"));
        assertEquals(1, control.currentAttempt());
        assertEquals(2, control.requestRestart(1, "again"));
        assertEquals(2, control.currentAttempt());
    }

    @Test
    void step3_acknowledgementsAreKeptPerCheckpoint() {
        var control = Impl.jobControl(ops, newJob("control"));
        var s0 = source(0, 1, "7-0");
        var k2 = keyed(2, 1, "s1p2/ckpt-1/state");
        control.acknowledge(0, s0);
        control.acknowledge(0, k2);
        control.acknowledge(0, source(0, 2, "9-0"));

        Map<PartitionId, TaskSnapshot> acks = control.acknowledgements(1);
        assertEquals(Set.of(new PartitionId(0, 0), new PartitionId(1, 2)), acks.keySet());
        Snapshots.assertSame(s0, acks.get(new PartitionId(0, 0)));
        Snapshots.assertSame(k2, acks.get(new PartitionId(1, 2)));
        assertEquals(1, control.acknowledgements(2).size());
        assertTrue(control.acknowledgements(3).isEmpty());
    }

    @Test
    void step3_acknowledgementFromAnOldAttemptIsIgnored() {
        var control = Impl.jobControl(ops, newJob("control"));
        control.requestRestart(1, "restart");
        control.acknowledge(0, source(0, 1, "7-0")); // a zombie task from attempt 0
        assertTrue(control.acknowledgements(1).isEmpty());
        control.acknowledge(1, source(0, 1, "7-0"));
        assertEquals(1, control.acknowledgements(1).size());
    }

    @Test
    void step3_declinesAreRecordedWithTheirReason() {
        var control = Impl.jobControl(ops, newJob("control"));
        control.decline(0, 1, new PartitionId(1, 4), "disk full");
        assertEquals(Map.of(new PartitionId(1, 4), "disk full"), control.declines(1));
        assertTrue(control.declines(2).isEmpty());
    }

    @Test
    void step3_anOldLeaderCannotRequestCheckpointsOrRestarts() throws Exception {
        String job = newJob("control");
        var control = Impl.jobControl(ops, job);
        var a = Impl.leaderElection(ops, job, "a", Duration.ofMillis(300));
        var b = Impl.leaderElection(ops, job, "b", Duration.ofMillis(300));
        long ea = a.acquireOrRenew().orElseThrow();
        Thread.sleep(450);
        long eb = b.acquireOrRenew().orElseThrow();

        assertThrows(FencedException.class, () -> control.requestCheckpoint(ea, new CheckpointBarrier(5, 5)));
        assertThrows(FencedException.class, () -> control.requestRestart(ea, "zombie"));
        assertEquals(Optional.empty(), control.requestedCheckpoint());
        assertEquals(0, control.currentAttempt());

        control.requestCheckpoint(eb, new CheckpointBarrier(6, 6));
        assertEquals(6, control.requestedCheckpoint().orElseThrow().checkpointId());
    }
}

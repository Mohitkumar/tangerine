package io.redstream.runtime.checkpoint;

import static io.redstream.runtime.checkpoint.Snapshots.metadata;
import static io.redstream.runtime.checkpoint.Snapshots.source;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.redis.FencedException;
import io.redstream.runtime.Impl;
import io.redstream.runtime.RedisTestBase;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Leader election and leader fencing (plan step 3). */
class LeaderElectionIT extends RedisTestBase {

    private static final Duration TTL = Duration.ofMillis(300);

    @Test
    void step3_onlyOneLeaderAtATime() {
        String job = newJob("leader");
        var a = Impl.leaderElection(ops, job, "a", TTL);
        var b = Impl.leaderElection(ops, job, "b", TTL);
        assertTrue(a.acquireOrRenew().isPresent());
        assertTrue(b.acquireOrRenew().isEmpty(), "b cannot lead while a holds the lease");
        assertEquals(Optional.of("a"), a.currentLeader());
        assertEquals(Optional.of("a"), b.currentLeader());
    }

    @Test
    void step3_renewingKeepsTheEpoch() {
        var a = Impl.leaderElection(ops, newJob("leader"), "a", TTL);
        long first = a.acquireOrRenew().orElseThrow();
        assertEquals(first, a.acquireOrRenew().orElseThrow());
    }

    @Test
    void step3_takeoverAfterTtlBumpsTheEpoch() throws Exception {
        String job = newJob("leader");
        var a = Impl.leaderElection(ops, job, "a", TTL);
        var b = Impl.leaderElection(ops, job, "b", TTL);
        long ea = a.acquireOrRenew().orElseThrow();
        Thread.sleep(TTL.toMillis() + 150); // a stalls past its TTL
        long eb = b.acquireOrRenew().orElseThrow();
        assertTrue(eb > ea, "new leader, higher epoch");
        assertTrue(a.acquireOrRenew().isEmpty(), "the old leader does not get it back while b holds it");
        assertEquals(Optional.of("b"), a.currentLeader());
    }

    @Test
    void step3_releaseHandsOverWithoutWaiting() {
        String job = newJob("leader");
        var a = Impl.leaderElection(ops, job, "a", Duration.ofSeconds(30));
        var b = Impl.leaderElection(ops, job, "b", Duration.ofSeconds(30));
        a.acquireOrRenew().orElseThrow();
        assertTrue(a.release());
        assertTrue(b.acquireOrRenew().isPresent());
        assertTrue(!a.release(), "a no longer holds it");
    }

    @Test
    void step3_noLeaderBeforeAnyoneAsks() {
        assertTrue(Impl.leaderElection(ops, newJob("leader"), "a", TTL).currentLeader().isEmpty());
    }

    @Test
    void step3_oldLeaderCannotCommitACheckpoint() throws Exception {
        String job = newJob("leader");
        var a = Impl.leaderElection(ops, job, "a", TTL);
        var b = Impl.leaderElection(ops, job, "b", TTL);
        CheckpointStorage storage = Impl.redisCheckpointStorage(ops, job);
        long ea = a.acquireOrRenew().orElseThrow();
        Thread.sleep(TTL.toMillis() + 150);
        long eb = b.acquireOrRenew().orElseThrow();

        var zombie = metadata(1, false, source(0, 1, "1-0"));
        assertThrows(FencedException.class, () -> storage.commitCheckpoint(ea, zombie));
        assertTrue(storage.latestCompleted().isEmpty(), "the fenced commit wrote nothing");

        storage.commitCheckpoint(eb, metadata(2, false, source(0, 2, "2-0")));
        assertEquals(2, storage.latestCompleted().orElseThrow().checkpointId());
    }
}

package io.redstream.coord;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Picks one worker to act as the job's coordinator, the role Flink's JobManager plays. Works
 * like a partition lease: a TTL key holds the leader's id, and every new leader bumps a leader
 * epoch that fences writes made by a leader that has since lost the lease.
 *
 * <p>Plan step 3. Hint: reuse {@code lease.lua} with job-level keys under the {@code {job}}
 * hash tag, so leader-fenced writes to the job's checkpoint keys can be one atomic script.
 */
public interface LeaderElection {

    /** Becomes or stays leader. Returns the leader epoch to write with, or empty if another worker leads. */
    OptionalLong acquireOrRenew();

    /** Gives up leadership if still held, so another worker can take over without waiting for the TTL. */
    boolean release();

    /** Worker id of the current leader, if any. */
    Optional<String> currentLeader();
}

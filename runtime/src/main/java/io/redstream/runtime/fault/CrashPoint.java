package io.redstream.runtime.fault;

/**
 * Places in a task where a crash is interesting for exactly-once testing. Call
 * {@link FaultHooks#at} at each one from {@code StreamTask}; add more as the plan grows.
 */
public enum CrashPoint {
    /** Records read and partly processed. */
    AFTER_READ,
    /** State snapshotted for a checkpoint; acknowledgement not sent (plan step 4). */
    BEFORE_ACK,
    /** Acknowledgement sent; checkpoint may not be complete yet (plan step 4). */
    AFTER_ACK,
    /** Checkpoint complete; sink transaction not committed yet (plan step 7). */
    BEFORE_SINK_COMMIT
}

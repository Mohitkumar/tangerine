package io.redstream.runtime.fault;

/** Places in the task loop where a crash is interesting for exactly-once testing. */
public enum CrashPoint {
    /** Records read and partly processed; nothing committed. */
    AFTER_READ,
    /** Batch fully processed in memory; commit not sent. */
    BEFORE_COMMIT,
    /** Commit applied; outbox not forwarded yet. */
    AFTER_COMMIT,
    /** Outbox entries appended downstream; forwarding progress not acked (duplicates follow). */
    BEFORE_FORWARD_ACK
}

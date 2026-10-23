package io.redstream.api;

import java.time.Duration;
import java.util.Objects;

/**
 * How often the job takes a checkpoint, like Flink's {@code env.enableCheckpointing(...)}.
 *
 * @param interval        time between the start of one checkpoint and the next
 * @param timeout         a checkpoint not acknowledged by every task within this is aborted
 * @param minPauseBetween least time between one checkpoint completing and the next starting
 */
public record CheckpointConfig(Duration interval, Duration timeout, Duration minPauseBetween) {

    public CheckpointConfig {
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(minPauseBetween, "minPauseBetween");
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("checkpoint interval must be positive");
        }
    }

    /** A checkpoint every {@code interval}, timing out after 10 minutes, no forced pause. */
    public static CheckpointConfig every(Duration interval) {
        return new CheckpointConfig(interval, Duration.ofMinutes(10), Duration.ZERO);
    }
}

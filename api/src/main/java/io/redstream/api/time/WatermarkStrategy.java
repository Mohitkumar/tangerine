package io.redstream.api.time;

import java.time.Duration;
import java.util.Objects;

/**
 * How a source derives event time and watermarks. Watermark = max timestamp seen minus the
 * allowed out-of-orderness.
 */
public record WatermarkStrategy<T>(TimestampAssigner<T> timestampAssigner, Duration maxOutOfOrderness, boolean enabled) {

    public WatermarkStrategy {
        Objects.requireNonNull(timestampAssigner, "timestampAssigner");
        Objects.requireNonNull(maxOutOfOrderness, "maxOutOfOrderness");
        if (maxOutOfOrderness.isNegative()) {
            throw new IllegalArgumentException("maxOutOfOrderness must be >= 0");
        }
    }

    /** Uses the Redis entry timestamp as event time and never emits watermarks. */
    public static <T> WatermarkStrategy<T> noWatermarks() {
        return new WatermarkStrategy<>((e, ts) -> ts, Duration.ZERO, false);
    }

    public static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
        return new WatermarkStrategy<>((e, ts) -> ts, maxOutOfOrderness, true);
    }

    public WatermarkStrategy<T> withTimestampAssigner(TimestampAssigner<T> assigner) {
        return new WatermarkStrategy<>(assigner, maxOutOfOrderness, enabled);
    }
}

package io.redstream.api.window;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/** Fixed-size, non-overlapping event-time windows aligned to the epoch. */
public final class TumblingEventTimeWindows<T> implements WindowAssigner<T> {

    private final long sizeMillis;

    private TumblingEventTimeWindows(long sizeMillis) {
        if (sizeMillis <= 0) {
            throw new IllegalArgumentException("window size must be > 0");
        }
        this.sizeMillis = sizeMillis;
    }

    public static <T> TumblingEventTimeWindows<T> of(Duration size) {
        return new TumblingEventTimeWindows<>(size.toMillis());
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp) {
        long start = Math.floorDiv(timestamp, sizeMillis) * sizeMillis;
        return List.of(new TimeWindow(start, start + sizeMillis));
    }

    public long sizeMillis() {
        return sizeMillis;
    }
}

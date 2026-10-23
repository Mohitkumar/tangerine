package io.redstream.api.window;

/** Half-open event-time interval {@code [start, end)}. */
public record TimeWindow(long start, long end) {

    public TimeWindow {
        if (end <= start) {
            throw new IllegalArgumentException("end must be > start");
        }
    }

    /** Last timestamp that belongs to this window; the window fires when the watermark passes it. */
    public long maxTimestamp() {
        return end - 1;
    }
}

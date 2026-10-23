package io.redstream.plan;

import java.util.stream.IntStream;

/** Inclusive range of key groups. */
public record KeyGroupRange(int start, int end) {

    public KeyGroupRange {
        if (start < 0 || end < start - 1) {
            throw new IllegalArgumentException("invalid range [" + start + ", " + end + "]");
        }
    }

    public boolean contains(int keyGroup) {
        return keyGroup >= start && keyGroup <= end;
    }

    public int size() {
        return end - start + 1;
    }

    public IntStream stream() {
        return IntStream.rangeClosed(start, end);
    }
}

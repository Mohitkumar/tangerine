package io.redstream.plan;

/**
 * Key groups are the fixed unit of keyed state, input, commit and ownership. A keyed stage has
 * exactly {@code maxParallelism} of them for its whole life; workers own sets of key groups,
 * so rescaling moves whole groups and never splits one (same idea as Flink's key groups and
 * Kafka's fixed partition count).
 */
public final class KeyGroups {

    private KeyGroups() {}

    public static int keyGroupFor(byte[] serializedKey, int maxParallelism) {
        return Math.floorMod(Murmur3.hash32(serializedKey), maxParallelism);
    }

    /**
     * Contiguous, balanced range for operator {@code index} of {@code parallelism}; sizes differ
     * by at most one. Same formula as Flink's KeyGroupRangeAssignment.
     */
    public static KeyGroupRange rangeFor(int maxParallelism, int parallelism, int index) {
        if (parallelism < 1 || parallelism > maxParallelism) {
            throw new IllegalArgumentException("parallelism must be in [1, maxParallelism]");
        }
        if (index < 0 || index >= parallelism) {
            throw new IllegalArgumentException("index out of range");
        }
        int start = (index * maxParallelism + parallelism - 1) / parallelism;
        int end = ((index + 1) * maxParallelism - 1) / parallelism;
        return new KeyGroupRange(start, end);
    }
}

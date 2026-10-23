package io.redstream.coord;

import java.util.ArrayList;
import java.util.List;

/**
 * Decides which worker should own each partition, given the live worker list. Every worker
 * computes this independently from the same inputs, so they agree without talking to each
 * other. Leases, not this function, are the source of truth: a disagreement during a
 * membership change only delays a handover, it cannot cause two owners.
 *
 * <p>Round-robin over the global partition list keeps each worker's share within one of the
 * average and spreads every stage across workers.
 */
public final class Assignment {

    private Assignment() {}

    /**
     * @param partitionsPerStage partition count of each stage, in stage order
     * @return every partition of the job in a fixed global order
     */
    public static List<PartitionId> allPartitions(int[] partitionsPerStage) {
        List<PartitionId> all = new ArrayList<>();
        for (int s = 0; s < partitionsPerStage.length; s++) {
            for (int p = 0; p < partitionsPerStage[s]; p++) {
                all.add(new PartitionId(s, p));
            }
        }
        return List.copyOf(all);
    }

    /** @param globalIndex the partition's position in {@link #allPartitions} */
    public static String preferredOwner(int globalIndex, List<String> liveWorkers) {
        if (liveWorkers.isEmpty()) {
            throw new IllegalArgumentException("no live workers");
        }
        return liveWorkers.get(globalIndex % liveWorkers.size());
    }
}

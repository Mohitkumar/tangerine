package io.redstream.runtime.checkpoint;

import java.util.List;

/**
 * A completed checkpoint: one {@link TaskSnapshot} for every partition of every stage, like
 * Flink's {@code _metadata} file. Recovery reads the latest one and restores each task from it.
 *
 * @param savepoint {@code true} if triggered by hand to stop, upgrade or rescale the job
 *                  (plan step 8); savepoints are never deleted automatically
 */
public record CheckpointMetadata(long checkpointId, long triggeredAtMillis, long completedAtMillis, boolean savepoint,
                                 List<TaskSnapshot> tasks) {

    public CheckpointMetadata {
        tasks = List.copyOf(tasks);
    }
}

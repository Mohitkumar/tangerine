package io.redstream.runtime.checkpoint;

/**
 * Told when a checkpoint completes or is aborted, like Flink's {@code CheckpointListener}.
 * Completion is the signal for side effects that must happen only once the checkpoint is safe,
 * such as committing a sink transaction or deleting files an older checkpoint no longer needs.
 */
public interface CheckpointListener {

    void notifyCheckpointComplete(long checkpointId) throws Exception;

    default void notifyCheckpointAborted(long checkpointId) throws Exception {}
}

package io.redstream.runtime.checkpoint;

import io.redstream.coord.PartitionId;
import java.util.Map;
import java.util.Optional;

/**
 * The job-level state shared through Redis between the coordinator and the tasks. In Flink
 * these messages travel over RPC between the JobManager and TaskManagers; here every worker
 * reads and writes them in Redis, since workers only coordinate through Redis.
 *
 * <p>Two numbers drive everything:
 *
 * <ul>
 *   <li>the <b>requested checkpoint</b>: sources poll it and inject a barrier when it moves;</li>
 *   <li>the <b>attempt</b>: bumped on every global restart. Tasks tag their acknowledgements
 *       with the attempt they run in, so acks from tasks of an old attempt are ignored. Flink
 *       uses execution attempt ids the same way.</li>
 * </ul>
 *
 * <p>Plan step 3. The attempt starts at 0. Writes made by the coordinator are fenced by its
 * leader epoch the same way as {@link CheckpointStorage#commitCheckpoint}: a lower epoch than
 * the current one throws {@code FencedException}.
 */
public interface JobControl {

    // Coordinator -> tasks

    /** Asks sources to start checkpoint {@code barrier.checkpointId()}. */
    void requestCheckpoint(long leaderEpoch, CheckpointBarrier barrier);

    /** The most recently requested checkpoint, if any. Sources poll this between batches. */
    Optional<CheckpointBarrier> requestedCheckpoint();

    /** Starts a new attempt: every worker stops its tasks and restores from the latest checkpoint. */
    long requestRestart(long leaderEpoch, String reason);

    /** The current attempt; a worker whose tasks run an older attempt must restart them. */
    long currentAttempt();

    // Tasks -> coordinator

    /** A task has snapshotted its state for a checkpoint. Ignored if {@code attempt} is not current. */
    void acknowledge(long attempt, TaskSnapshot snapshot);

    /** A task could not take part in a checkpoint; the coordinator aborts it. */
    void decline(long attempt, long checkpointId, PartitionId partition, String reason);

    /** Acknowledgements received so far for a checkpoint. */
    Map<PartitionId, TaskSnapshot> acknowledgements(long checkpointId);

    /** Declines received so far for a checkpoint, with their reasons. */
    Map<PartitionId, String> declines(long checkpointId);
}

package io.redstream.runtime.checkpoint;

import io.redstream.coord.PartitionId;
import io.redstream.runtime.state.KeyedStateHandle;
import java.util.Map;
import java.util.Objects;

/**
 * What one task contributes to a checkpoint, sent to the coordinator as its acknowledgement.
 * Flink calls this the task's state snapshot: keyed state plus operator state.
 *
 * @param partition     the task's partition
 * @param checkpointId  the checkpoint this belongs to
 * @param sourceOffset  for a source task, the last input entry id included in the checkpoint
 *                      (restore resumes after it); {@code null} for a keyed task
 * @param watermark     the task's watermark at the barrier
 * @param keyedState    for a keyed task, its state; {@code null} for a source task
 * @param operatorState other named per-task state, such as a two-phase-commit sink's pending
 *                      transactions (plan step 7); empty if none
 */
public record TaskSnapshot(PartitionId partition, long checkpointId, String sourceOffset, long watermark,
                           KeyedStateHandle keyedState, Map<String, byte[]> operatorState) {

    public TaskSnapshot {
        Objects.requireNonNull(partition, "partition");
        operatorState = Map.copyOf(operatorState);
    }
}

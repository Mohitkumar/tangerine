package io.redstream.runtime.checkpoint;

import io.redstream.redis.StreamEntry;
import java.util.List;
import java.util.Optional;

/**
 * Aligns barriers for a task that reads from several upstream producers, like Flink's
 * {@code CheckpointBarrierHandler} for aligned checkpoints.
 *
 * <p>A keyed task's single input stream mixes entries from every upstream partition (the
 * {@code p} field says which). Once barrier {@code n} has arrived from producer A, A's later
 * records belong to checkpoint {@code n + 1} and must wait: processing them now would leak
 * them into checkpoint {@code n}'s state. The task snapshots when barrier {@code n} has arrived
 * from every producer, then processes what was held back.
 *
 * <p>Plan step 5. Construct an implementation with the set of producers this task reads from.
 * A source task has no upstream producers and never needs alignment.
 */
public interface CheckpointBarrierHandler {

    /**
     * A data entry arrived from {@code producer}. Returns {@code true} if the task may process it
     * now, {@code false} if the handler buffered it because that producer's barrier has already
     * arrived for the checkpoint being aligned.
     */
    boolean onData(String producer, StreamEntry entry);

    /**
     * A barrier arrived from {@code producer}. Returns the barrier once it has arrived from every
     * producer (the task must then snapshot and acknowledge), otherwise empty.
     *
     * <p>Hint: a barrier for a newer checkpoint than the one being aligned means the older one
     * was aborted; decide whether to cancel it or keep waiting, and test the case.
     */
    Optional<CheckpointBarrier> onBarrier(String producer, CheckpointBarrier barrier);

    /** After snapshotting an aligned checkpoint: the entries held back during alignment, in arrival order. */
    List<StreamEntry> drainBuffered();
}

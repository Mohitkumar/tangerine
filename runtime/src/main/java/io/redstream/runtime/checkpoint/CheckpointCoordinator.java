package io.redstream.runtime.checkpoint;

/**
 * Runs on the leader and drives checkpoints, like Flink's {@code CheckpointCoordinator}:
 *
 * <ol>
 *   <li>trigger: pick the next id and publish it through {@link JobControl#requestCheckpoint};</li>
 *   <li>collect: wait until every partition of every stage has acknowledged;</li>
 *   <li>complete: build {@link CheckpointMetadata} from the acks and commit it to
 *       {@link CheckpointStorage}; then discard checkpoints that are no longer needed;</li>
 *   <li>abort: on a decline or a timeout, drop the pending checkpoint. The job keeps running.</li>
 * </ol>
 *
 * <p>Create one coordinator per leadership term, with that term's leader epoch, and close it
 * when leadership is lost. Plan step 3. Hint: keep at most one checkpoint pending at a time.
 * Flink allows more, but one is easier to reason about and is Flink's default.
 *
 * <p>Tests drive it without {@link #start()}: they call {@link #triggerCheckpoint} and
 * {@link #poll} directly, with a fake clock.
 */
public interface CheckpointCoordinator extends AutoCloseable {

    /** Starts triggering every {@code CheckpointConfig.interval()} and polling, on a background thread. */
    void start();

    /** Triggers a checkpoint now and returns its id; {@code savepoint} marks it as one (plan step 8). */
    long triggerCheckpoint(boolean savepoint);

    /**
     * One step of the state machine: complete the pending checkpoint if every partition has
     * acknowledged, abort it on a decline or a timeout, or keep waiting. An aborted checkpoint
     * never completes, even if its missing acknowledgements arrive later.
     */
    void poll();

    /** Stops triggering, for example after losing leadership. */
    @Override
    void close();
}

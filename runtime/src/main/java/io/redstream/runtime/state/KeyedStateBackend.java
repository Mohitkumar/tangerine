package io.redstream.runtime.state;

import io.redstream.redis.Bytes;
import io.redstream.runtime.checkpoint.CheckpointListener;

/**
 * The keyed state of one key group, like Flink's {@code KeyedStateBackend}. Writes go straight
 * into the backend with no per-batch buffer, because consistency comes from snapshots taken at
 * checkpoint barriers, not from a commit per batch.
 *
 * <p>{@code ValueState} handles (plan step 1) read and write through this interface, with state
 * keys built by {@link StateKeys}. Tests: {@code HeapStateBackendTest}, {@code RocksDbStateBackendTest}.
 */
public interface KeyedStateBackend extends CheckpointListener, AutoCloseable {

    void setCurrentKey(byte[] serializedKey);

    /** The current key; throws {@code IllegalStateException} if no key has been set yet. */
    byte[] currentKey();

    /** Returns the value, or {@code null} if absent. */
    byte[] get(Bytes stateKey);

    void put(Bytes stateKey, byte[] value);

    void delete(Bytes stateKey);

    /**
     * Writes a snapshot of all state to checkpoint storage and returns a handle to it. Called by
     * the task when checkpoint barrier {@code checkpointId} is aligned, with no record being
     * processed, so the state does not change while this runs.
     *
     * <p>Start synchronous. Flink splits this into a short synchronous part and an asynchronous
     * upload, which is a later optimization (plan stretch goals).
     */
    KeyedStateHandle snapshot(long checkpointId) throws Exception;

    /** Called once every task has acknowledged {@code checkpointId}; RocksDB uses it to drop old local files. */
    @Override
    default void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @Override
    void close();
}

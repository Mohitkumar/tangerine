package io.redstream.runtime.checkpoint;

import java.util.Optional;

/**
 * Where checkpoints are kept, like Flink's {@code CheckpointStorage}. Holds two things: blobs
 * written by state backends, and the metadata of completed checkpoints.
 *
 * <p>Plan step 2 implements it on Redis, which suits small state the way Flink's
 * JobManager-memory storage does. A filesystem or S3 implementation can come later without
 * changing callers.
 *
 * <p>Hints for the Redis version: put the metadata and the "latest" pointer under the
 * {@code {job}} hash tag so {@link #commitCheckpoint} can be one fenced Lua script. Blobs can
 * live anywhere; split large ones into chunks (Redis values are capped at 512 MB, and big values
 * block the server while they are written).
 */
public interface CheckpointStorage {

    void putBlob(String name, byte[] data);

    /** Returns the blob; throws if it does not exist, because a missing blob means a corrupt checkpoint. */
    byte[] getBlob(String name);

    void deleteBlob(String name);

    /**
     * Records a checkpoint as complete and makes it the latest, in one atomic step. Fenced by
     * the leader epoch: if {@code leaderEpoch} is lower than the job's current leader epoch (the
     * one {@code LeaderElection} last handed out, 0 before any), it throws
     * {@code FencedException} and writes nothing.
     */
    void commitCheckpoint(long leaderEpoch, CheckpointMetadata metadata);

    /** The newest completed checkpoint or savepoint, if any. */
    Optional<CheckpointMetadata> latestCompleted();

    Optional<CheckpointMetadata> load(long checkpointId);

    /**
     * Deletes a checkpoint's metadata and every blob no newer checkpoint still uses. Hint: with
     * incremental RocksDB checkpoints a blob can be shared, so count references before deleting.
     */
    void discard(long checkpointId);
}

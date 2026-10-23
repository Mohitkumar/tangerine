package io.redstream.runtime.checkpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CheckpointStorage} in memory, for state backend tests. It records the name of every
 * blob written, so a test can check what a snapshot uploaded. It ignores leader epochs.
 */
public final class InMemoryCheckpointStorage implements CheckpointStorage {

    private final Map<String, byte[]> blobs = new ConcurrentHashMap<>();
    private final List<String> putLog = new ArrayList<>();
    private final TreeMap<Long, CheckpointMetadata> checkpoints = new TreeMap<>();

    /** Names passed to {@link #putBlob}, in order, including overwrites. */
    public synchronized List<String> putLog() {
        return List.copyOf(putLog);
    }

    public boolean hasBlob(String name) {
        return blobs.containsKey(name);
    }

    @Override
    public synchronized void putBlob(String name, byte[] data) {
        blobs.put(name, data.clone());
        putLog.add(name);
    }

    @Override
    public byte[] getBlob(String name) {
        byte[] b = blobs.get(name);
        if (b == null) {
            throw new NoSuchElementException("no blob " + name);
        }
        return b.clone();
    }

    @Override
    public void deleteBlob(String name) {
        blobs.remove(name);
    }

    @Override
    public synchronized void commitCheckpoint(long leaderEpoch, CheckpointMetadata metadata) {
        checkpoints.put(metadata.checkpointId(), metadata);
    }

    @Override
    public synchronized Optional<CheckpointMetadata> latestCompleted() {
        return checkpoints.isEmpty() ? Optional.empty() : Optional.of(checkpoints.lastEntry().getValue());
    }

    @Override
    public synchronized Optional<CheckpointMetadata> load(long checkpointId) {
        return Optional.ofNullable(checkpoints.get(checkpointId));
    }

    @Override
    public synchronized void discard(long checkpointId) {
        checkpoints.remove(checkpointId);
    }
}

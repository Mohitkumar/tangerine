package io.redstream.runtime.state;

import java.util.List;
import java.util.Objects;

/**
 * Points at one key group's state inside a checkpoint, like Flink's {@code KeyedStateHandle}.
 * The state itself lives in checkpoint storage as named blobs.
 *
 * <ul>
 *   <li>Heap backend: one blob holding the whole serialized map.</li>
 *   <li>RocksDB backend: one blob per SST file plus the files RocksDB needs to open them. With
 *       incremental checkpoints, a blob can be shared by several checkpoints (SST files never
 *       change once written), so blobs are not owned by a single checkpoint.</li>
 * </ul>
 *
 * @param backend        {@link StateBackend#name()} of the backend that wrote it; restore checks it
 * @param blobs          names of the blobs in {@code CheckpointStorage}
 * @param stateSizeBytes total size, for logs and metrics
 */
public record KeyedStateHandle(String backend, List<String> blobs, long stateSizeBytes) {

    public KeyedStateHandle {
        Objects.requireNonNull(backend, "backend");
        blobs = List.copyOf(blobs);
    }
}

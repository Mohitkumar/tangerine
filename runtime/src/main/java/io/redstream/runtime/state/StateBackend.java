package io.redstream.runtime.state;

import io.redstream.coord.PartitionId;
import io.redstream.runtime.checkpoint.CheckpointStorage;
import java.nio.file.Path;

/**
 * Creates the keyed state of a key group, like Flink's {@code StateBackend}. Two
 * implementations are planned, matching Flink's:
 *
 * <ul>
 *   <li>{@code HashMapStateBackend}: state in a Java map on the heap (plan step 2);</li>
 *   <li>{@code EmbeddedRocksDBStateBackend}: state in RocksDB on local disk (plan step 6).</li>
 * </ul>
 */
public interface StateBackend {

    /** Short name stored in each {@link KeyedStateHandle}, e.g. {@code "hashmap"} or {@code "rocksdb"}. */
    String name();

    /**
     * Creates the state of one key group.
     *
     * @param restoreFrom the key group's state in the last completed checkpoint, or {@code null}
     *                    to start empty
     */
    KeyedStateBackend createKeyedStateBackend(Context context, KeyedStateHandle restoreFrom) throws Exception;

    /**
     * What a backend needs to know about the key group it serves.
     *
     * @param localDir a directory owned by this key group on this worker; RocksDB keeps its files
     *                 here, the heap backend ignores it
     * @param storage  where snapshots are written to and restored from
     */
    record Context(String jobName, PartitionId partition, Path localDir, CheckpointStorage storage) {}
}

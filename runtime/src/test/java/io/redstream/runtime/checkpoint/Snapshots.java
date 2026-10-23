package io.redstream.runtime.checkpoint;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.redstream.coord.PartitionId;
import io.redstream.runtime.state.KeyedStateHandle;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/** Test data and comparisons for checkpoint records. Records holding byte arrays need a field-by-field compare. */
public final class Snapshots {

    private Snapshots() {}

    public static TaskSnapshot source(int partition, long checkpointId, String offset) {
        return new TaskSnapshot(new PartitionId(0, partition), checkpointId, offset, 1_000L, null, Map.of());
    }

    public static TaskSnapshot keyed(int partition, long checkpointId, String... blobs) {
        return new TaskSnapshot(new PartitionId(1, partition), checkpointId, null, 2_000L,
                new KeyedStateHandle("hashmap", List.of(blobs), 42L),
                Map.of("pending-txn", ("txn-" + checkpointId).getBytes(StandardCharsets.UTF_8)));
    }

    public static CheckpointMetadata metadata(long id, boolean savepoint, TaskSnapshot... tasks) {
        return new CheckpointMetadata(id, 100L * id, 100L * id + 50, savepoint, List.of(tasks));
    }

    public static void assertSame(TaskSnapshot expected, TaskSnapshot actual) {
        assertEquals(expected.partition(), actual.partition());
        assertEquals(expected.checkpointId(), actual.checkpointId());
        assertEquals(expected.sourceOffset(), actual.sourceOffset());
        assertEquals(expected.watermark(), actual.watermark());
        assertEquals(expected.keyedState(), actual.keyedState());
        assertEquals(expected.operatorState().keySet(), actual.operatorState().keySet());
        expected.operatorState().forEach((k, v) -> assertArrayEquals(v, actual.operatorState().get(k), "operator state " + k));
    }

    public static void assertSame(CheckpointMetadata expected, CheckpointMetadata actual) {
        assertEquals(expected.checkpointId(), actual.checkpointId());
        assertEquals(expected.triggeredAtMillis(), actual.triggeredAtMillis());
        assertEquals(expected.completedAtMillis(), actual.completedAtMillis());
        assertEquals(expected.savepoint(), actual.savepoint());
        assertEquals(expected.tasks().size(), actual.tasks().size(), "task count");
        for (int i = 0; i < expected.tasks().size(); i++) {
            assertSame(expected.tasks().get(i), actual.tasks().get(i));
        }
    }
}

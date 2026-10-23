package io.redstream.runtime.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.coord.PartitionId;
import io.redstream.redis.Bytes;
import io.redstream.runtime.checkpoint.InMemoryCheckpointStorage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What every {@link StateBackend} must do. Each test name starts with the plan step that makes
 * it pass. Subclasses only say which backend to test.
 */
abstract class KeyedStateBackendContract {

    @TempDir
    Path dir;

    final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final List<KeyedStateBackend> opened = new ArrayList<>();

    abstract StateBackend backend();

    /** A new backend for key group 3 of stage 1, each in its own local directory, as on a new worker. */
    KeyedStateBackend create(KeyedStateHandle restoreFrom) throws Exception {
        var ctx = new StateBackend.Context("contract-job", new PartitionId(1, 3), dir.resolve("instance-" + opened.size()), storage);
        KeyedStateBackend b = backend().createKeyedStateBackend(ctx, restoreFrom);
        opened.add(b);
        return b;
    }

    @AfterEach
    void closeAll() {
        opened.forEach(KeyedStateBackend::close);
    }

    static Bytes key(String state, String key) {
        return StateKeys.encode(1, state, key.getBytes(StandardCharsets.UTF_8));
    }

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- Step 1: reads and writes ---------------------------------------------------------

    @Test
    void step1_missingValueIsNull() throws Exception {
        assertNull(create(null).get(key("count", "a")));
    }

    @Test
    void step1_putThenGet() throws Exception {
        var b = create(null);
        b.put(key("count", "a"), utf8("1"));
        assertArrayEquals(utf8("1"), b.get(key("count", "a")));
    }

    @Test
    void step1_putOverwrites() throws Exception {
        var b = create(null);
        b.put(key("count", "a"), utf8("1"));
        b.put(key("count", "a"), utf8("2"));
        assertArrayEquals(utf8("2"), b.get(key("count", "a")));
    }

    @Test
    void step1_deleteRemoves() throws Exception {
        var b = create(null);
        b.put(key("count", "a"), utf8("1"));
        b.delete(key("count", "a"));
        assertNull(b.get(key("count", "a")));
    }

    @Test
    void step1_deleteOfMissingKeyIsFine() throws Exception {
        var b = create(null);
        b.delete(key("count", "nothing"));
        assertNull(b.get(key("count", "nothing")));
    }

    @Test
    void step1_keysAndStateNamesAreIndependent() throws Exception {
        var b = create(null);
        b.put(key("count", "a"), utf8("1"));
        b.put(key("count", "b"), utf8("2"));
        b.put(key("sum", "a"), utf8("3"));
        assertArrayEquals(utf8("1"), b.get(key("count", "a")));
        assertArrayEquals(utf8("2"), b.get(key("count", "b")));
        assertArrayEquals(utf8("3"), b.get(key("sum", "a")));
    }

    @Test
    void step1_emptyValueIsNotMissing() throws Exception {
        var b = create(null);
        b.put(key("flag", "a"), new byte[0]);
        assertArrayEquals(new byte[0], b.get(key("flag", "a")));
    }

    @Test
    void step1_readsDoNotDependOnTheCallersArray() throws Exception {
        var b = create(null);
        byte[] value = utf8("1");
        b.put(key("count", "a"), value);
        value[0] = 'X';
        assertArrayEquals(utf8("1"), b.get(key("count", "a")), "put must not keep a reference the caller can change");
    }

    @Test
    void step1_currentKey() throws Exception {
        var b = create(null);
        assertThrows(IllegalStateException.class, b::currentKey, "no key set yet");
        b.setCurrentKey(utf8("a"));
        assertArrayEquals(utf8("a"), b.currentKey());
        b.setCurrentKey(utf8("b"));
        assertArrayEquals(utf8("b"), b.currentKey());
    }

    // ---- Step 2: snapshot and restore -----------------------------------------------------

    @Test
    void step2_restoreGivesTheSameState() throws Exception {
        var b = create(null);
        for (int i = 0; i < 500; i++) {
            b.put(key("count", "k" + i), utf8(Integer.toString(i)));
        }
        for (int i = 0; i < 500; i += 5) {
            b.delete(key("count", "k" + i));
        }
        KeyedStateHandle handle = b.snapshot(1);

        var restored = create(handle);
        for (int i = 0; i < 500; i++) {
            byte[] v = restored.get(key("count", "k" + i));
            if (i % 5 == 0) {
                assertNull(v, "k" + i + " was deleted before the snapshot");
            } else {
                assertArrayEquals(utf8(Integer.toString(i)), v, "k" + i);
            }
        }
    }

    @Test
    void step2_handleNamesTheBackendAndItsBlobsExist() throws Exception {
        var b = create(null);
        b.put(key("count", "a"), utf8("1"));
        KeyedStateHandle handle = b.snapshot(1);
        assertEquals(backend().name(), handle.backend());
        assertTrue(!handle.blobs().isEmpty(), "a snapshot writes at least one blob");
        handle.blobs().forEach(name -> assertTrue(storage.hasBlob(name), "blob " + name + " is in storage"));
    }

    @Test
    void step2_snapshotIsNotChangedByLaterWrites() throws Exception {
        var b = create(null);
        b.put(key("count", "a"), utf8("1"));
        KeyedStateHandle first = b.snapshot(1);
        b.put(key("count", "a"), utf8("2"));
        b.put(key("count", "b"), utf8("9"));
        KeyedStateHandle second = b.snapshot(2);

        var fromFirst = create(first);
        assertArrayEquals(utf8("1"), fromFirst.get(key("count", "a")));
        assertNull(fromFirst.get(key("count", "b")));

        var fromSecond = create(second);
        assertArrayEquals(utf8("2"), fromSecond.get(key("count", "a")));
        assertArrayEquals(utf8("9"), fromSecond.get(key("count", "b")));
    }

    @Test
    void step2_restoredBackendKeepsWorking() throws Exception {
        var b = create(null);
        b.put(key("count", "a"), utf8("1"));
        var restored = create(b.snapshot(1));
        restored.put(key("count", "a"), utf8("2"));
        var again = create(restored.snapshot(2));
        assertArrayEquals(utf8("2"), again.get(key("count", "a")));
    }

    @Test
    void step2_emptyStateRoundTrips() throws Exception {
        var restored = create(create(null).snapshot(1));
        assertNull(restored.get(key("count", "a")));
    }

    @Test
    void step2_binaryKeysAndValuesRoundTrip() throws Exception {
        var b = create(null);
        Bytes binaryKey = StateKeys.encode(7, "raw", new byte[] {0, -1, 0, 10, 13});
        byte[] binaryValue = {0, 0, -128, 127, 0};
        b.put(binaryKey, binaryValue);
        b.put(key("flag", "empty"), new byte[0]);
        var restored = create(b.snapshot(1));
        assertArrayEquals(binaryValue, restored.get(binaryKey));
        assertArrayEquals(new byte[0], restored.get(key("flag", "empty")));
    }

    @Test
    void step2_restoreRejectsAHandleFromAnotherBackend() {
        StateBackend backend = backend(); // outside assertThrows, so "not wired yet" still skips
        var foreign = new KeyedStateHandle("some-other-backend", List.of(), 0);
        var ctx = new StateBackend.Context("contract-job", new PartitionId(1, 3), dir.resolve("foreign"), storage);
        assertThrows(Exception.class, () -> backend.createKeyedStateBackend(ctx, foreign).close());
    }
}

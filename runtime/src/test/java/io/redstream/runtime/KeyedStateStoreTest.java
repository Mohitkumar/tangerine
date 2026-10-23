package io.redstream.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.redis.Bytes;
import io.redstream.runtime.state.KeyedStateStore;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KeyedStateStoreTest {

    private static final Bytes A = Bytes.utf8("a");
    private static final Bytes B = Bytes.utf8("b");

    @Test
    void readsSeeOwnWritesAndDeletesOverCommittedValues() {
        var store = new KeyedStateStore(Map.of(A, new byte[] {1}, B, new byte[] {2}));
        store.put(A, new byte[] {9});
        store.delete(B);
        assertArrayEquals(new byte[] {9}, store.get(A));
        assertNull(store.get(B));
        assertEquals(1, store.pendingPuts().size());
        assertTrue(store.pendingDeletes().contains(B));
    }

    @Test
    void commitFoldsOverlayIntoCommittedView() {
        var store = new KeyedStateStore(Map.of(B, new byte[] {2}));
        store.put(A, new byte[] {1});
        store.delete(B);
        store.onCommitted();
        assertTrue(store.pendingPuts().isEmpty());
        assertTrue(store.pendingDeletes().isEmpty());
        assertArrayEquals(new byte[] {1}, store.get(A));
        assertNull(store.get(B));
    }

    @Test
    void putAfterDeleteRevivesKey() {
        var store = new KeyedStateStore(Map.of());
        store.delete(A);
        store.put(A, new byte[] {3});
        assertArrayEquals(new byte[] {3}, store.get(A));
        assertTrue(store.pendingDeletes().isEmpty());
    }
}

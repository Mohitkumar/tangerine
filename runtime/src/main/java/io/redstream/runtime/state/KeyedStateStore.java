package io.redstream.runtime.state;

import io.redstream.redis.Bytes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A key group's state: the committed values loaded at restore, plus this batch's uncommitted
 * writes and deletes. Reads see the batch's own writes. On commit the overlay is folded in;
 * on failure the whole task is discarded and restored from Redis, so there is no rollback path.
 *
 * <p>Milestone 1 loads the full key group at restore. Lazy loading with a bounded cache is a
 * later optimization; the commit protocol does not change.
 */
public final class KeyedStateStore {

    private final Map<Bytes, byte[]> committed;
    private final Map<Bytes, byte[]> puts = new HashMap<>();
    private final Set<Bytes> deletes = new HashSet<>();
    private byte[] currentKey;

    public KeyedStateStore(Map<Bytes, byte[]> committed) {
        this.committed = new HashMap<>(committed);
    }

    public void setCurrentKey(byte[] serializedKey) {
        this.currentKey = serializedKey;
    }

    public byte[] currentKey() {
        if (currentKey == null) {
            throw new IllegalStateException("no current key: state used outside processElement");
        }
        return currentKey;
    }

    public byte[] get(Bytes stateKey) {
        if (deletes.contains(stateKey)) {
            return null;
        }
        byte[] v = puts.get(stateKey);
        return v != null ? v : committed.get(stateKey);
    }

    public void put(Bytes stateKey, byte[] value) {
        deletes.remove(stateKey);
        puts.put(stateKey, value);
    }

    public void delete(Bytes stateKey) {
        puts.remove(stateKey);
        deletes.add(stateKey);
    }

    public Map<Bytes, byte[]> pendingPuts() {
        return puts;
    }

    public Set<Bytes> pendingDeletes() {
        return deletes;
    }

    /** Folds the batch into the committed view after a successful commit. */
    public void onCommitted() {
        committed.putAll(puts);
        deletes.forEach(committed::remove);
        puts.clear();
        deletes.clear();
    }
}

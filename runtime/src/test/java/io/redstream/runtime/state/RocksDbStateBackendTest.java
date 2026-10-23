package io.redstream.runtime.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.runtime.Impl;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The RocksDB backend (plan step 6): the same contract as the heap backend, plus incremental
 * checkpoints. These tests assume blob names of SST files end in {@code .sst}, and that an SST
 * file keeps the same blob name in every checkpoint that uses it.
 */
class RocksDbStateBackendTest extends KeyedStateBackendContract {

    @Override
    StateBackend backend() {
        return Impl.rocksDbStateBackend();
    }

    private static List<String> sst(List<String> names) {
        return names.stream().filter(n -> n.endsWith(".sst")).toList();
    }

    private void fill(KeyedStateBackend b, int from, int to) {
        for (int i = from; i < to; i++) {
            b.put(key("count", "k" + i), utf8("value-" + i));
        }
    }

    @Test
    void step6_secondSnapshotOfUnchangedStateUploadsNoNewSstFiles() throws Exception {
        var b = create(null);
        fill(b, 0, 5_000);
        b.snapshot(1);
        int before = storage.putLog().size();

        KeyedStateHandle second = b.snapshot(2);

        List<String> uploaded = storage.putLog().subList(before, storage.putLog().size());
        assertEquals(List.of(), sst(uploaded), "unchanged SST files are not uploaded again");
        assertTrue(!sst(second.blobs()).isEmpty(), "the handle still lists the SST files it needs");
    }

    @Test
    void step6_incrementalHandleReusesEarlierSstBlobs() throws Exception {
        var b = create(null);
        fill(b, 0, 5_000);
        KeyedStateHandle first = b.snapshot(1);
        fill(b, 5_000, 10_000);
        KeyedStateHandle second = b.snapshot(2);

        assertTrue(second.blobs().containsAll(sst(first.blobs())), "old SST files are shared, not copied");
        assertTrue(sst(second.blobs()).size() > sst(first.blobs()).size(), "the second checkpoint adds new SST files");
    }

    @Test
    void step6_restoreFromAnIncrementalCheckpoint() throws Exception {
        var b = create(null);
        fill(b, 0, 5_000);
        b.snapshot(1);
        fill(b, 5_000, 10_000);
        b.delete(key("count", "k42"));
        var restored = create(b.snapshot(2));

        assertArrayEquals(utf8("value-1"), restored.get(key("count", "k1")));
        assertArrayEquals(utf8("value-9999"), restored.get(key("count", "k9999")));
        assertEquals(null, restored.get(key("count", "k42")));
    }
}

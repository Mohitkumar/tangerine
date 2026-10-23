package io.redstream.runtime.checkpoint;

import static io.redstream.runtime.checkpoint.Snapshots.keyed;
import static io.redstream.runtime.checkpoint.Snapshots.metadata;
import static io.redstream.runtime.checkpoint.Snapshots.source;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.runtime.Impl;
import io.redstream.runtime.RedisTestBase;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Checkpoint storage on Redis (plan step 2; shared blobs in step 6). */
class RedisCheckpointStorageIT extends RedisTestBase {

    private CheckpointStorage storage() {
        return Impl.redisCheckpointStorage(ops, newJob("storage"));
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void step2_blobRoundTrip() {
        var s = storage();
        s.putBlob("s1p3/ckpt-1/state", utf8("hello"));
        assertArrayEquals(utf8("hello"), s.getBlob("s1p3/ckpt-1/state"));
    }

    @Test
    void step2_putOverwrites() {
        var s = storage();
        s.putBlob("b", utf8("one"));
        s.putBlob("b", utf8("two"));
        assertArrayEquals(utf8("two"), s.getBlob("b"));
    }

    @Test
    void step2_missingBlobThrows() {
        var s = storage();
        assertThrows(RuntimeException.class, () -> s.getBlob("never-written"));
    }

    @Test
    void step2_deletedBlobIsGone() {
        var s = storage();
        s.putBlob("b", utf8("x"));
        s.deleteBlob("b");
        assertThrows(RuntimeException.class, () -> s.getBlob("b"));
    }

    @Test
    void step2_largeAndBinaryBlobsRoundTrip() {
        var s = storage();
        byte[] big = new byte[5 * 1024 * 1024 + 17];
        new Random(1).nextBytes(big);
        s.putBlob("big", big);
        assertArrayEquals(big, s.getBlob("big"));
        s.putBlob("empty", new byte[0]);
        assertArrayEquals(new byte[0], s.getBlob("empty"));
    }

    @Test
    void step2_nothingCompletedYet() {
        assertTrue(storage().latestCompleted().isEmpty());
    }

    @Test
    void step2_commitMakesTheCheckpointLatest() {
        var s = storage();
        var first = metadata(1, false, source(0, 1, "5-0"), keyed(0, 1, "s1p0/ckpt-1/state"));
        var second = metadata(2, false, source(0, 2, "9-0"), keyed(0, 2, "s1p0/ckpt-2/state"));
        s.commitCheckpoint(1, first);
        Snapshots.assertSame(first, s.latestCompleted().orElseThrow());
        s.commitCheckpoint(1, second);
        Snapshots.assertSame(second, s.latestCompleted().orElseThrow());
    }

    @Test
    void step2_loadById() {
        var s = storage();
        var first = metadata(1, false, source(0, 1, "5-0"));
        s.commitCheckpoint(1, first);
        s.commitCheckpoint(1, metadata(2, true, source(0, 2, "6-0")));
        Snapshots.assertSame(first, s.load(1).orElseThrow());
        assertTrue(s.load(2).orElseThrow().savepoint());
        assertTrue(s.load(3).isEmpty());
    }

    @Test
    void step2_discardRemovesMetadataAndItsBlobs() {
        var s = storage();
        s.putBlob("a", utf8("a"));
        s.putBlob("b", utf8("b"));
        s.commitCheckpoint(1, metadata(1, false, keyed(0, 1, "a")));
        s.commitCheckpoint(1, metadata(2, false, keyed(0, 2, "b")));

        s.discard(1);

        assertTrue(s.load(1).isEmpty());
        assertThrows(RuntimeException.class, () -> s.getBlob("a"));
        assertArrayEquals(utf8("b"), s.getBlob("b"));
        assertEquals(2, s.latestCompleted().orElseThrow().checkpointId());
    }

    @Test
    void step6_discardKeepsBlobsANewerCheckpointStillUses() {
        var s = storage();
        s.putBlob("shared.sst", utf8("s"));
        s.putBlob("only-1.sst", utf8("1"));
        s.putBlob("only-2.sst", utf8("2"));
        s.commitCheckpoint(1, metadata(1, false, keyed(0, 1, "shared.sst", "only-1.sst")));
        s.commitCheckpoint(1, metadata(2, false, keyed(0, 2, "shared.sst", "only-2.sst")));

        s.discard(1);

        assertArrayEquals(utf8("s"), s.getBlob("shared.sst"), "still used by checkpoint 2");
        assertThrows(RuntimeException.class, () -> s.getBlob("only-1.sst"));
    }
}

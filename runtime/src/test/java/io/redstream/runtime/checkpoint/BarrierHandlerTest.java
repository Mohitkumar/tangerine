package io.redstream.runtime.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.redis.StreamEntry;
import io.redstream.runtime.Impl;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Barrier alignment (plan step 5). Producers are named like {@code PartitionKeys.producerId()}. */
class BarrierHandlerTest {

    private static final CheckpointBarrier B1 = new CheckpointBarrier(1, 100);
    private static final CheckpointBarrier B2 = new CheckpointBarrier(2, 200);

    private int seq;

    private StreamEntry entry(String producer) {
        seq++;
        return new StreamEntry(seq + "-0", Map.of("p", producer.getBytes(StandardCharsets.UTF_8),
                "v", ("r" + seq).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void step5_oneProducerAlignsAtOnce() {
        var h = Impl.barrierHandler(Set.of("s0p0"));
        assertTrue(h.onData("s0p0", entry("s0p0")));
        assertEquals(Optional.of(B1), h.onBarrier("s0p0", B1));
        assertEquals(List.of(), h.drainBuffered());
    }

    @Test
    void step5_waitsForEveryProducer() {
        var h = Impl.barrierHandler(Set.of("s0p0", "s0p1", "s0p2"));
        assertEquals(Optional.empty(), h.onBarrier("s0p0", B1));
        assertEquals(Optional.empty(), h.onBarrier("s0p2", B1));
        assertEquals(Optional.of(B1), h.onBarrier("s0p1", B1));
    }

    @Test
    void step5_recordsAfterAProducersBarrierWaitOthersFlow() {
        var h = Impl.barrierHandler(Set.of("s0p0", "s0p1"));
        h.onBarrier("s0p0", B1);

        StreamEntry early0 = entry("s0p0");
        StreamEntry fromP1 = entry("s0p1");
        StreamEntry late0 = entry("s0p0");
        assertFalse(h.onData("s0p0", early0), "s0p0 is past barrier 1: hold it back");
        assertTrue(h.onData("s0p1", fromP1), "s0p1 has not sent barrier 1: still checkpoint 1");
        assertFalse(h.onData("s0p0", late0));

        assertEquals(Optional.of(B1), h.onBarrier("s0p1", B1));
        assertEquals(List.of(early0, late0), h.drainBuffered(), "held-back records, in arrival order");
        assertEquals(List.of(), h.drainBuffered(), "draining empties the buffer");
        assertTrue(h.onData("s0p0", entry("s0p0")), "after alignment everything flows again");
    }

    @Test
    void step5_consecutiveCheckpointsAlign() {
        var h = Impl.barrierHandler(Set.of("s0p0", "s0p1"));
        h.onBarrier("s0p0", B1);
        assertEquals(Optional.of(B1), h.onBarrier("s0p1", B1));
        h.drainBuffered();

        assertEquals(Optional.empty(), h.onBarrier("s0p1", B2));
        assertFalse(h.onData("s0p1", entry("s0p1")));
        assertTrue(h.onData("s0p0", entry("s0p0")));
        assertEquals(Optional.of(B2), h.onBarrier("s0p0", B2));
        assertEquals(1, h.drainBuffered().size());
    }

    @Test
    void step5_dataBeforeAnyBarrierFlows() {
        var h = Impl.barrierHandler(Set.of("s0p0", "s0p1"));
        for (int i = 0; i < 10; i++) {
            assertTrue(h.onData(i % 2 == 0 ? "s0p0" : "s0p1", entry(i % 2 == 0 ? "s0p0" : "s0p1")));
        }
        assertEquals(List.of(), h.drainBuffered());
    }
}

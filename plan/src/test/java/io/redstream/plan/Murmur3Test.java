package io.redstream.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class Murmur3Test {

    @Test
    void matchesReferenceVectors() {
        assertEquals(0, Murmur3.hash32(new byte[0]));
        assertEquals(0x248bfa47, Murmur3.hash32("hello".getBytes(StandardCharsets.UTF_8)));
        assertEquals(0x2e4ff723, Murmur3.hash32("The quick brown fox jumps over the lazy dog".getBytes(StandardCharsets.UTF_8)));
    }
}

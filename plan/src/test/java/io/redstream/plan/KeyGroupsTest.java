package io.redstream.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class KeyGroupsTest {

    @Test
    void rangesCoverAllKeyGroupsWithoutOverlap() {
        for (int max : new int[] {1, 7, 128, 1000}) {
            for (int p = 1; p <= Math.min(max, 17); p++) {
                Set<Integer> covered = new HashSet<>();
                int total = 0;
                for (int i = 0; i < p; i++) {
                    var r = KeyGroups.rangeFor(max, p, i);
                    assertTrue(r.size() >= max / p && r.size() <= max / p + 1, "balanced range size");
                    total += r.size();
                    r.stream().forEach(covered::add);
                }
                assertEquals(max, total);
                assertEquals(max, covered.size());
            }
        }
    }

    @Test
    void keyGroupIsStableAndInRange() {
        byte[] key = "user-42".getBytes(StandardCharsets.UTF_8);
        int kg = KeyGroups.keyGroupFor(key, 128);
        assertTrue(kg >= 0 && kg < 128);
        assertEquals(kg, KeyGroups.keyGroupFor(key.clone(), 128));
    }
}

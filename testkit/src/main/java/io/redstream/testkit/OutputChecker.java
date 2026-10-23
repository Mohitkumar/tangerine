package io.redstream.testkit;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Compares a job's final per-key results with a reference computed in memory. Exactly-once
 * means: same keys, same final values, no matter where crashes were injected.
 */
public final class OutputChecker {

    private OutputChecker() {}

    /** Returns a readable diff, or an empty string if the maps are equal. */
    public static <K extends Comparable<K>, V> String diff(Map<K, V> expected, Map<K, V> actual) {
        StringBuilder sb = new StringBuilder();
        Map<K, V> all = new TreeMap<>(expected);
        Map<K, V> extra = new HashMap<>(actual);
        all.forEach((k, v) -> {
            V a = extra.remove(k);
            if (!Objects.equals(v, a)) {
                sb.append(k).append(": expected ").append(v).append(", got ").append(a).append('\n');
            }
        });
        new TreeMap<>(extra).forEach((k, v) -> sb.append(k).append(": unexpected ").append(v).append('\n'));
        return sb.toString();
    }
}

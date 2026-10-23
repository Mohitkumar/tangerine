package io.redstream.redis;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Immutable byte array with value equality, for use as a map key. */
public final class Bytes implements Comparable<Bytes> {

    private final byte[] data;

    private Bytes(byte[] data) {
        this.data = data;
    }

    public static Bytes wrap(byte[] data) {
        return new Bytes(data.clone());
    }

    public static Bytes utf8(String s) {
        return new Bytes(s.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] toArray() {
        return data.clone();
    }

    /** Returns the backing array without copying; callers must not mutate it. */
    public byte[] unsafeArray() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Bytes b && Arrays.equals(data, b.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public int compareTo(Bytes o) {
        return Arrays.compareUnsigned(data, o.data);
    }

    @Override
    public String toString() {
        return new String(data, StandardCharsets.UTF_8);
    }
}

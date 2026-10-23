package io.redstream.plan;

/**
 * MurmurHash3 x86_32. Used to map serialized keys to key groups. It must give the same result
 * on every JVM forever, which is why keys are hashed from their bytes and never via
 * {@code Object.hashCode()} (enum and identity hash codes differ between processes).
 */
public final class Murmur3 {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private Murmur3() {}

    public static int hash32(byte[] data) {
        return hash32(data, 0);
    }

    public static int hash32(byte[] data, int seed) {
        int h = seed;
        int len = data.length;
        int i = 0;
        while (i + 4 <= len) {
            int k = (data[i] & 0xff) | (data[i + 1] & 0xff) << 8 | (data[i + 2] & 0xff) << 16 | (data[i + 3] & 0xff) << 24;
            h ^= mixK(k);
            h = Integer.rotateLeft(h, 13);
            h = h * 5 + 0xe6546b64;
            i += 4;
        }
        int k = 0;
        switch (len & 3) {
            case 3:
                k ^= (data[i + 2] & 0xff) << 16;
            case 2:
                k ^= (data[i + 1] & 0xff) << 8;
            case 1:
                k ^= data[i] & 0xff;
                h ^= mixK(k);
            default:
                break;
        }
        h ^= len;
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    private static int mixK(int k) {
        k *= C1;
        k = Integer.rotateLeft(k, 15);
        return k * C2;
    }
}

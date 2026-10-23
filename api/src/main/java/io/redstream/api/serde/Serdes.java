package io.redstream.api.serde;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Function;

/** Built-in serdes for common types. */
public final class Serdes {

    private Serdes() {}

    public static Serde<String> string() {
        return of(s -> s.getBytes(StandardCharsets.UTF_8), b -> new String(b, StandardCharsets.UTF_8));
    }

    public static Serde<Long> longs() {
        return of(v -> ByteBuffer.allocate(Long.BYTES).putLong(v).array(), b -> ByteBuffer.wrap(b).getLong());
    }

    public static Serde<Integer> ints() {
        return of(v -> ByteBuffer.allocate(Integer.BYTES).putInt(v).array(), b -> ByteBuffer.wrap(b).getInt());
    }

    public static Serde<byte[]> bytes() {
        return of(Function.identity(), Function.identity());
    }

    /** Builds a serde from two functions, e.g. for a record encoded as text or JSON. */
    public static <T> Serde<T> of(Function<T, byte[]> serializer, Function<byte[], T> deserializer) {
        Objects.requireNonNull(serializer, "serializer");
        Objects.requireNonNull(deserializer, "deserializer");
        return new Serde<>() {
            @Override
            public byte[] serialize(T value) {
                return serializer.apply(value);
            }

            @Override
            public T deserialize(byte[] bytes) {
                return deserializer.apply(bytes);
            }
        };
    }
}

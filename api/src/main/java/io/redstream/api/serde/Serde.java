package io.redstream.api.serde;

/**
 * Converts values to and from bytes. Every type-changing operator takes an explicit
 * {@code Serde} because Java erases generic types at runtime: the engine cannot pick a
 * serializer for a {@code DataStream<Event>} by itself.
 *
 * <p>Key serdes must be deterministic: equal keys must produce identical bytes, because
 * the key group is computed from the serialized key, on every worker.
 */
public interface Serde<T> {

    byte[] serialize(T value);

    T deserialize(byte[] bytes);
}

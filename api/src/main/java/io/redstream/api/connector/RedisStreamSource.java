package io.redstream.api.connector;

import io.redstream.api.serde.Serde;
import java.util.List;
import java.util.Objects;

/**
 * Reads one or more existing Redis streams. Each stream is one source partition, the same way
 * a Kafka topic partition is one unit of parallelism. The value is read from {@code payloadField}.
 */
public record RedisStreamSource<T>(List<String> streams, Serde<T> serde, String payloadField, StartPosition startPosition) {

    public enum StartPosition { EARLIEST, LATEST }

    public RedisStreamSource {
        streams = List.copyOf(streams);
        Objects.requireNonNull(serde, "serde");
        Objects.requireNonNull(payloadField, "payloadField");
        Objects.requireNonNull(startPosition, "startPosition");
        if (streams.isEmpty()) {
            throw new IllegalArgumentException("at least one source stream is required");
        }
    }

    public static <T> RedisStreamSource<T> of(Serde<T> serde, String... streams) {
        return new RedisStreamSource<>(List.of(streams), serde, "v", StartPosition.EARLIEST);
    }

    public RedisStreamSource<T> withPayloadField(String field) {
        return new RedisStreamSource<>(streams, serde, field, startPosition);
    }

    public RedisStreamSource<T> startingAt(StartPosition position) {
        return new RedisStreamSource<>(streams, serde, payloadField, position);
    }
}

package io.redstream.api.connector;

import io.redstream.api.serde.Serde;
import java.util.Objects;

/**
 * Appends results to a Redis stream. Each entry carries the producer id ({@code p}) and
 * sequence number ({@code s}) so a consumer can drop duplicates and get exactly-once
 * end to end; delivery into the stream itself is at-least-once.
 */
public record RedisStreamSink<T>(String stream, Serde<T> serde) {

    public RedisStreamSink {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(serde, "serde");
    }

    public static <T> RedisStreamSink<T> of(String stream, Serde<T> serde) {
        return new RedisStreamSink<>(stream, serde);
    }
}

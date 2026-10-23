package io.redstream.api.state;

import io.redstream.api.serde.Serde;
import java.util.Objects;

/** Names a piece of keyed state and says how to serialize it. Names must be unique per operator. */
public record ValueStateDescriptor<T>(String name, Serde<T> serde) {

    public ValueStateDescriptor {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(serde, "serde");
        if (name.isBlank()) {
            throw new IllegalArgumentException("state name must not be blank");
        }
    }
}

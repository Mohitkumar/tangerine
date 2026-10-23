package io.redstream.api.transformation;

import io.redstream.api.functions.KeySelector;
import io.redstream.api.serde.Serde;
import java.util.List;

/**
 * Not an operator: marks a hash-partitioned edge produced by {@code keyBy}. The planner starts
 * a new stage here, and records cross it through Redis instead of a method call.
 */
public record PartitionTransformation<T, K>(int id, String name, Transformation<T> input, KeySelector<T, K> keySelector, Serde<K> keySerde, Serde<T> valueSerde)
        implements Transformation<T> {

    @Override
    public List<Transformation<?>> inputs() {
        return List.of(input);
    }
}

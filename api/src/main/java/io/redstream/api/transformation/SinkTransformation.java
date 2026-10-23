package io.redstream.api.transformation;

import io.redstream.api.connector.RedisStreamSink;
import java.util.List;

public record SinkTransformation<T>(int id, String name, Transformation<T> input, RedisStreamSink<T> sink)
        implements Transformation<Void> {

    @Override
    public List<Transformation<?>> inputs() {
        return List.of(input);
    }
}

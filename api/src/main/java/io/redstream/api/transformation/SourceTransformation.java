package io.redstream.api.transformation;

import io.redstream.api.connector.RedisStreamSource;
import io.redstream.api.time.WatermarkStrategy;
import java.util.List;

public record SourceTransformation<T>(int id, String name, RedisStreamSource<T> source, WatermarkStrategy<T> watermarks)
        implements Transformation<T> {

    @Override
    public List<Transformation<?>> inputs() {
        return List.of();
    }
}

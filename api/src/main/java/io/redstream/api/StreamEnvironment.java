package io.redstream.api;

import io.redstream.api.connector.RedisStreamSource;
import io.redstream.api.time.WatermarkStrategy;
import io.redstream.api.transformation.SourceTransformation;
import io.redstream.api.transformation.Transformation;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for building a job. Calls on the returned streams only record transformations;
 * {@link #build} freezes them into a {@link JobDefinition} that a worker runs.
 *
 * <pre>{@code
 * var env = StreamEnvironment.create();
 * env.fromSource(RedisStreamSource.of(Serdes.string(), "lines:0", "lines:1"), WatermarkStrategy.noWatermarks(), "lines")
 *    .flatMap("split", (line, out) -> ..., Serdes.string())
 *    .keyBy(w -> w, Serdes.string())
 *    .process("count", new CountFn(), countSerde)
 *    .sinkTo(RedisStreamSink.of("counts", countSerde));
 * JobDefinition job = env.build("wordcount");
 * }</pre>
 */
public final class StreamEnvironment {

    public static final int DEFAULT_MAX_PARALLELISM = 128;
    public static final int MAX_MAX_PARALLELISM = 1 << 15;

    private final List<Transformation<?>> transformations = new ArrayList<>();
    private int nextId;
    private int maxParallelism = DEFAULT_MAX_PARALLELISM;

    private StreamEnvironment() {}

    public static StreamEnvironment create() {
        return new StreamEnvironment();
    }

    /**
     * Sets the number of key groups for keyed stages. This bounds how many workers can share a
     * keyed stage and cannot change without migrating state, so pick it generously up front.
     */
    public StreamEnvironment maxParallelism(int keyGroups) {
        if (keyGroups < 1 || keyGroups > MAX_MAX_PARALLELISM) {
            throw new IllegalArgumentException("maxParallelism must be in [1, " + MAX_MAX_PARALLELISM + "]");
        }
        this.maxParallelism = keyGroups;
        return this;
    }

    public <T> DataStream<T> fromSource(RedisStreamSource<T> source, WatermarkStrategy<T> watermarks, String name) {
        var t = add(new SourceTransformation<>(newId(), name, source, watermarks));
        return new DataStream<>(this, t, source.serde());
    }

    public JobDefinition build(String jobName) {
        return new JobDefinition(jobName, maxParallelism, transformations);
    }

    int newId() {
        return nextId++;
    }

    <T extends Transformation<?>> T add(T transformation) {
        transformations.add(transformation);
        return transformation;
    }
}

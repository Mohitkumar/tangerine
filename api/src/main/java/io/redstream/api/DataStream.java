package io.redstream.api;

import io.redstream.api.connector.RedisStreamSink;
import io.redstream.api.functions.FilterFunction;
import io.redstream.api.functions.FlatMapFunction;
import io.redstream.api.functions.KeySelector;
import io.redstream.api.functions.MapFunction;
import io.redstream.api.serde.Serde;
import io.redstream.api.transformation.OneInputTransformation;
import io.redstream.api.transformation.OperatorSpec;
import io.redstream.api.transformation.PartitionTransformation;
import io.redstream.api.transformation.SinkTransformation;
import io.redstream.api.transformation.Transformation;
import java.util.Objects;

/**
 * A stream of records of type {@code T}. Methods return new streams and record a
 * transformation; nothing runs until a worker executes the built job.
 *
 * <p>Functions passed to {@code map}, {@code flatMap}, {@code filter} and {@code keyBy} are
 * shared by every partition a worker runs, concurrently. Keep them stateless. Per-partition
 * state belongs in a {@link io.redstream.api.functions.KeyedProcessFunction}.
 */
public class DataStream<T> {

    protected final StreamEnvironment env;
    protected final Transformation<T> transformation;
    protected final Serde<T> serde;

    DataStream(StreamEnvironment env, Transformation<T> transformation, Serde<T> serde) {
        this.env = env;
        this.transformation = transformation;
        this.serde = serde;
    }

    public <R> DataStream<R> map(String name, MapFunction<T, R> fn, Serde<R> outSerde) {
        return oneInput(name, new OperatorSpec.MapOp<>(Objects.requireNonNull(fn)), outSerde);
    }

    public <R> DataStream<R> map(MapFunction<T, R> fn, Serde<R> outSerde) {
        return map("map", fn, outSerde);
    }

    public <R> DataStream<R> flatMap(String name, FlatMapFunction<T, R> fn, Serde<R> outSerde) {
        return oneInput(name, new OperatorSpec.FlatMapOp<>(Objects.requireNonNull(fn)), outSerde);
    }

    public <R> DataStream<R> flatMap(FlatMapFunction<T, R> fn, Serde<R> outSerde) {
        return flatMap("flatMap", fn, outSerde);
    }

    public DataStream<T> filter(String name, FilterFunction<T> fn) {
        return oneInput(name, new OperatorSpec.FilterOp<>(Objects.requireNonNull(fn)), serde);
    }

    public DataStream<T> filter(FilterFunction<T> fn) {
        return filter("filter", fn);
    }

    /**
     * Hash-partitions the stream by key. Records with equal serialized keys always reach the same
     * key group, which owns their state. This ends the current stage: records cross to the next
     * stage through Redis.
     */
    public <K> KeyedStream<K, T> keyBy(KeySelector<T, K> keySelector, Serde<K> keySerde) {
        var t = env.add(new PartitionTransformation<>(env.newId(), "keyBy", transformation,
                Objects.requireNonNull(keySelector), Objects.requireNonNull(keySerde), serde));
        return new KeyedStream<>(env, t, serde);
    }

    public DataStreamSink sinkTo(RedisStreamSink<T> sink) {
        env.add(new SinkTransformation<>(env.newId(), "sink:" + sink.stream(), transformation, sink));
        return new DataStreamSink();
    }

    public Transformation<T> transformation() {
        return transformation;
    }

    public Serde<T> serde() {
        return serde;
    }

    <R> DataStream<R> oneInput(String name, OperatorSpec<T, R> spec, Serde<R> outSerde) {
        var t = env.add(new OneInputTransformation<>(env.newId(), name, transformation, spec, Objects.requireNonNull(outSerde)));
        return new DataStream<>(env, t, outSerde);
    }
}

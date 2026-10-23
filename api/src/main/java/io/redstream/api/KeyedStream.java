package io.redstream.api;

import io.redstream.api.functions.KeyedProcessFunction;
import io.redstream.api.serde.Serde;
import io.redstream.api.transformation.OneInputTransformation;
import io.redstream.api.transformation.OperatorSpec;
import io.redstream.api.transformation.PartitionTransformation;
import io.redstream.api.window.WindowAssigner;
import java.util.Objects;
import java.util.function.Supplier;

/** A stream partitioned by key. Only keyed streams can use keyed state, timers and windows. */
public final class KeyedStream<K, T> {

    private final StreamEnvironment env;
    private final PartitionTransformation<T, K> partition;
    private final Serde<T> serde;

    KeyedStream(StreamEnvironment env, PartitionTransformation<T, K> partition, Serde<T> serde) {
        this.env = env;
        this.partition = partition;
        this.serde = serde;
    }

    /**
     * Applies a keyed function. {@code factory} is called once per partition (key group) and
     * must return a new instance every time, e.g. {@code CountWords::new}: functions keep state
     * handles in fields, and a worker runs many partitions in one JVM.
     */
    public <R> DataStream<R> process(String name, Supplier<? extends KeyedProcessFunction<K, T, R>> factory, Serde<R> outSerde) {
        var t = env.add(new OneInputTransformation<>(env.newId(), name, partition,
                new OperatorSpec.KeyedProcessOp<>(Objects.requireNonNull(factory)), Objects.requireNonNull(outSerde)));
        return new DataStream<>(env, t, outSerde);
    }

    public <R> DataStream<R> process(Supplier<? extends KeyedProcessFunction<K, T, R>> factory, Serde<R> outSerde) {
        return process("process", factory, outSerde);
    }

    public WindowedStream<K, T> window(WindowAssigner<T> assigner) {
        return new WindowedStream<>(env, partition, serde, Objects.requireNonNull(assigner));
    }

    public PartitionTransformation<T, K> transformation() {
        return partition;
    }
}

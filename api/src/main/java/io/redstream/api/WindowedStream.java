package io.redstream.api;

import io.redstream.api.functions.ReduceFunction;
import io.redstream.api.serde.Serde;
import io.redstream.api.transformation.OneInputTransformation;
import io.redstream.api.transformation.OperatorSpec;
import io.redstream.api.transformation.PartitionTransformation;
import io.redstream.api.window.WindowAssigner;
import java.time.Duration;
import java.util.Objects;

/** A keyed stream grouped into event-time windows. Results are emitted when the watermark passes a window's end. */
public final class WindowedStream<K, T> {

    private final StreamEnvironment env;
    private final PartitionTransformation<T, K> partition;
    private final Serde<T> serde;
    private final WindowAssigner<T> assigner;
    private Duration allowedLateness = Duration.ZERO;

    WindowedStream(StreamEnvironment env, PartitionTransformation<T, K> partition, Serde<T> serde, WindowAssigner<T> assigner) {
        this.env = env;
        this.partition = partition;
        this.serde = serde;
        this.assigner = assigner;
    }

    public WindowedStream<K, T> allowedLateness(Duration lateness) {
        this.allowedLateness = Objects.requireNonNull(lateness);
        return this;
    }

    public DataStream<T> reduce(String name, ReduceFunction<T> reduce) {
        var spec = new OperatorSpec.WindowReduceOp<>(assigner, Objects.requireNonNull(reduce), allowedLateness);
        var t = env.add(new OneInputTransformation<>(env.newId(), name, partition, spec, serde));
        return new DataStream<>(env, t, serde);
    }

    public DataStream<T> reduce(ReduceFunction<T> reduce) {
        return reduce("windowReduce", reduce);
    }
}

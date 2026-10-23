package io.redstream.api.transformation;

import io.redstream.api.functions.FilterFunction;
import io.redstream.api.functions.FlatMapFunction;
import io.redstream.api.functions.KeyedProcessFunction;
import io.redstream.api.functions.MapFunction;
import io.redstream.api.functions.ReduceFunction;
import io.redstream.api.window.WindowAssigner;
import java.time.Duration;
import java.util.function.Supplier;

/** What a {@link OneInputTransformation} does. The runtime maps each variant to an operator. */
public sealed interface OperatorSpec<I, O> {

    record MapOp<I, O>(MapFunction<I, O> fn) implements OperatorSpec<I, O> {}

    record FlatMapOp<I, O>(FlatMapFunction<I, O> fn) implements OperatorSpec<I, O> {}

    record FilterOp<T>(FilterFunction<T> fn) implements OperatorSpec<T, T> {}

    /**
     * Takes a supplier, not an instance: keyed functions hold state handles in fields, so each
     * partition needs its own instance (same reason Kafka Streams takes a ProcessorSupplier).
     */
    record KeyedProcessOp<K, I, O>(Supplier<? extends KeyedProcessFunction<K, I, O>> factory) implements OperatorSpec<I, O> {}

    record WindowReduceOp<T>(WindowAssigner<T> assigner, ReduceFunction<T> reduce, Duration allowedLateness)
            implements OperatorSpec<T, T> {}
}

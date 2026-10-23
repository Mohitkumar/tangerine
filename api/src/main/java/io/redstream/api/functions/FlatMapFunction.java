package io.redstream.api.functions;

@FunctionalInterface
public interface FlatMapFunction<T, R> {
    void flatMap(T value, Collector<R> out) throws Exception;
}

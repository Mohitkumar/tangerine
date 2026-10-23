package io.redstream.api.functions;

@FunctionalInterface
public interface ReduceFunction<T> {
    T reduce(T accumulator, T value) throws Exception;
}

package io.redstream.api.functions;

@FunctionalInterface
public interface MapFunction<T, R> {
    R map(T value) throws Exception;
}

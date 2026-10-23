package io.redstream.api.functions;

@FunctionalInterface
public interface FilterFunction<T> {
    boolean filter(T value) throws Exception;
}

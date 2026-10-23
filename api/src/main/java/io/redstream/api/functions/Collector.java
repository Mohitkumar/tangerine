package io.redstream.api.functions;

/** Emits zero or more records downstream from inside a user function. */
@FunctionalInterface
public interface Collector<T> {
    void collect(T record);
}

package io.redstream.api.functions;

/**
 * Extracts the key used for partitioning and keyed state. Must be deterministic: the same
 * record must always yield an equal key, on every worker, in every run.
 */
@FunctionalInterface
public interface KeySelector<T, K> {
    K getKey(T value) throws Exception;
}

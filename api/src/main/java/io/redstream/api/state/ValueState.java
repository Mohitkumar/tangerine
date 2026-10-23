package io.redstream.api.state;

/** A single value scoped to the current key. */
public interface ValueState<T> {

    /** Returns the value for the current key, or {@code null} if none. */
    T value();

    void update(T value);

    void clear();
}

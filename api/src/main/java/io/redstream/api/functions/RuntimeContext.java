package io.redstream.api.functions;

import io.redstream.api.state.ValueState;
import io.redstream.api.state.ValueStateDescriptor;

/** Handed to keyed functions in {@code open()}; the place to declare state. */
public interface RuntimeContext {

    String jobName();

    /**
     * Returns a handle to keyed state. The handle is bound to the key of whichever record is
     * being processed, so declare it once in {@code open()} and reuse it.
     */
    <T> ValueState<T> getState(ValueStateDescriptor<T> descriptor);
}

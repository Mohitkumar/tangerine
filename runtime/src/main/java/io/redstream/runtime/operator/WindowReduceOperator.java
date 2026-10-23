package io.redstream.runtime.operator;

import io.redstream.api.transformation.OperatorSpec;

/**
 * Milestone 4. Plan: per (key, window) accumulator in keyed state, one timer per window at
 * {@code window.maxTimestamp()}, fired when the key group's watermark passes it.
 */
final class WindowReduceOperator<T> implements Operator<T> {

    WindowReduceOperator(OperatorSpec.WindowReduceOp<T> spec, Operator<T> next) {}

    @Override
    public void open(TaskContext ctx) {
        throw new UnsupportedOperationException("windowed reduce is not implemented yet (milestone 4)");
    }

    @Override
    public void process(T value, long timestamp) {
        throw new IllegalStateException("not opened");
    }
}

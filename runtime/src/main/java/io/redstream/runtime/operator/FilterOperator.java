package io.redstream.runtime.operator;

import io.redstream.api.functions.FilterFunction;

final class FilterOperator<T> implements Operator<T> {

    private final FilterFunction<T> fn;
    private final Operator<T> next;

    FilterOperator(FilterFunction<T> fn, Operator<T> next) {
        this.fn = fn;
        this.next = next;
    }

    @Override
    public void process(T value, long timestamp) throws Exception {
        if (fn.filter(value)) {
            next.process(value, timestamp);
        }
    }
}

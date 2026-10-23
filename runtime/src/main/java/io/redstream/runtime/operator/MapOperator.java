package io.redstream.runtime.operator;

import io.redstream.api.functions.MapFunction;

final class MapOperator<I, O> implements Operator<I> {

    private final MapFunction<I, O> fn;
    private final Operator<O> next;

    MapOperator(MapFunction<I, O> fn, Operator<O> next) {
        this.fn = fn;
        this.next = next;
    }

    @Override
    public void process(I value, long timestamp) throws Exception {
        next.process(fn.map(value), timestamp);
    }
}

package io.redstream.runtime.operator;

import io.redstream.api.functions.FlatMapFunction;

final class FlatMapOperator<I, O> implements Operator<I> {

    private final FlatMapFunction<I, O> fn;
    private final Operator<O> next;

    FlatMapOperator(FlatMapFunction<I, O> fn, Operator<O> next) {
        this.fn = fn;
        this.next = next;
    }

    @Override
    public void process(I value, long timestamp) throws Exception {
        fn.flatMap(value, out -> Unchecked.run(() -> next.process(out, timestamp)));
    }
}

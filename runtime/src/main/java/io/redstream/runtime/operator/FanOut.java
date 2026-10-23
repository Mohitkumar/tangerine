package io.redstream.runtime.operator;

import java.util.List;

/** Sends each record to every downstream operator. */
final class FanOut<T> implements Operator<T> {

    private final List<Operator<T>> targets;

    FanOut(List<Operator<T>> targets) {
        this.targets = List.copyOf(targets);
    }

    @Override
    public void process(T value, long timestamp) throws Exception {
        for (Operator<T> t : targets) {
            t.process(value, timestamp);
        }
    }
}

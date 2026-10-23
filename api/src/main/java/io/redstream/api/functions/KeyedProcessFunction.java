package io.redstream.api.functions;

import io.redstream.api.time.TimerService;

/**
 * Low-level keyed operator with access to keyed state and event-time timers.
 *
 * <p>Everything a call to {@link #processElement} does to state and every record it emits are
 * committed atomically together with the input offset, so after a crash the function sees each
 * input record's effects exactly once.
 */
public abstract class KeyedProcessFunction<K, I, O> {

    /** Called once per partition before any record. Declare state here. */
    public void open(RuntimeContext ctx) throws Exception {}

    public abstract void processElement(I value, Context<K> ctx, Collector<O> out) throws Exception;

    /** Called when an event-time timer registered for the current key fires. */
    public void onTimer(long timestamp, Context<K> ctx, Collector<O> out) throws Exception {}

    public interface Context<K> {

        K getCurrentKey();

        /** Event timestamp of the current record, or the timer's timestamp in {@code onTimer}. */
        long timestamp();

        TimerService timerService();
    }
}

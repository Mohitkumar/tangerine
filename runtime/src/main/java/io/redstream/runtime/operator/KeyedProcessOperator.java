package io.redstream.runtime.operator;

import io.redstream.api.functions.KeySelector;
import io.redstream.api.functions.KeyedProcessFunction;
import io.redstream.api.functions.RuntimeContext;
import io.redstream.api.serde.Serde;
import io.redstream.api.state.ValueState;
import io.redstream.api.state.ValueStateDescriptor;
import io.redstream.api.time.TimerService;
import io.redstream.runtime.state.ValueStateImpl;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Supplier;

/** Runs a {@link KeyedProcessFunction}: sets the current key, then calls the function. */
final class KeyedProcessOperator<K, I, O> implements Operator<I> {

    /** Every function instance in use in this JVM, to catch suppliers that return a shared instance. */
    private static final Set<Object> INSTANCES = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    private final int operatorId;
    private final KeyedProcessFunction<K, I, O> fn;
    private final KeySelector<I, K> keySelector;
    private final Serde<K> keySerde;
    private final Operator<O> next;
    private TaskContext ctx;
    private K currentKey;
    private long currentTimestamp;

    KeyedProcessOperator(int operatorId, Supplier<? extends KeyedProcessFunction<K, I, O>> factory, KeySelector<I, K> keySelector,
                         Serde<K> keySerde, Operator<O> next) {
        this.operatorId = operatorId;
        this.fn = factory.get();
        if (fn == null || !INSTANCES.add(fn)) {
            throw new IllegalStateException("the KeyedProcessFunction supplier for operator " + operatorId
                    + " must return a new instance on every call (e.g. MyFunction::new), not a shared one");
        }
        this.keySelector = keySelector;
        this.keySerde = keySerde;
        this.next = next;
    }

    @Override
    public void open(TaskContext ctx) throws Exception {
        this.ctx = ctx;
        fn.open(new RuntimeContext() {
            @Override
            public String jobName() {
                return ctx.jobName();
            }

            @Override
            public <T> ValueState<T> getState(ValueStateDescriptor<T> descriptor) {
                return new ValueStateImpl<>(ctx.state(), operatorId, descriptor);
            }
        });
    }

    @Override
    public void process(I value, long timestamp) throws Exception {
        currentKey = keySelector.getKey(value);
        currentTimestamp = timestamp;
        ctx.state().setCurrentKey(keySerde.serialize(currentKey));
        fn.processElement(value, context, out -> Unchecked.run(() -> next.process(out, currentTimestamp)));
    }

    private final KeyedProcessFunction.Context<K> context = new KeyedProcessFunction.Context<>() {
        @Override
        public K getCurrentKey() {
            return currentKey;
        }

        @Override
        public long timestamp() {
            return currentTimestamp;
        }

        @Override
        public TimerService timerService() {
            return timers;
        }
    };

    // Event-time timers are milestone 4: a ZSET per key group, fired on watermark advance
    // inside commit.lua's transaction.
    private final TimerService timers = new TimerService() {
        @Override
        public long currentWatermark() {
            return ctx.watermark();
        }

        @Override
        public void registerEventTimeTimer(long timestamp) {
            throw new UnsupportedOperationException("event-time timers are not implemented yet (milestone 4)");
        }

        @Override
        public void deleteEventTimeTimer(long timestamp) {
            throw new UnsupportedOperationException("event-time timers are not implemented yet (milestone 4)");
        }
    };
}

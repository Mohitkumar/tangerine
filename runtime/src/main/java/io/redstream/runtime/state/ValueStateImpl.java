package io.redstream.runtime.state;

import io.redstream.api.state.ValueState;
import io.redstream.api.state.ValueStateDescriptor;

/** {@link ValueState} bound to whatever key the store's current key is. */
public final class ValueStateImpl<T> implements ValueState<T> {

    private final KeyedStateStore store;
    private final int operatorId;
    private final ValueStateDescriptor<T> descriptor;

    public ValueStateImpl(KeyedStateStore store, int operatorId, ValueStateDescriptor<T> descriptor) {
        this.store = store;
        this.operatorId = operatorId;
        this.descriptor = descriptor;
    }

    @Override
    public T value() {
        byte[] v = store.get(StateKeys.encode(operatorId, descriptor.name(), store.currentKey()));
        return v == null ? null : descriptor.serde().deserialize(v);
    }

    @Override
    public void update(T value) {
        if (value == null) {
            clear();
            return;
        }
        store.put(StateKeys.encode(operatorId, descriptor.name(), store.currentKey()), descriptor.serde().serialize(value));
    }

    @Override
    public void clear() {
        store.delete(StateKeys.encode(operatorId, descriptor.name(), store.currentKey()));
    }
}

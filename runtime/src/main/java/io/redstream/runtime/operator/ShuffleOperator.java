package io.redstream.runtime.operator;

import io.redstream.api.functions.KeySelector;
import io.redstream.api.serde.Serde;
import io.redstream.plan.KeyGroups;
import io.redstream.redis.OutRecord;

/**
 * End of a stage at a keyBy edge: computes the key group and appends the record to the outbox,
 * addressed to that key group's input stream in the next stage.
 */
final class ShuffleOperator<T, K> implements Operator<T> {

    private final int targetStage;
    private final KeySelector<T, K> keySelector;
    private final Serde<K> keySerde;
    private final Serde<T> valueSerde;
    private TaskContext ctx;

    ShuffleOperator(int targetStage, KeySelector<T, K> keySelector, Serde<K> keySerde, Serde<T> valueSerde) {
        this.targetStage = targetStage;
        this.keySelector = keySelector;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void open(TaskContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void process(T value, long timestamp) throws Exception {
        int keyGroup = KeyGroups.keyGroupFor(keySerde.serialize(keySelector.getKey(value)), ctx.maxParallelism());
        String destination = ctx.keySpace().partition(targetStage, keyGroup).in();
        ctx.emit(new OutRecord(destination, valueSerde.serialize(value), timestamp));
    }
}

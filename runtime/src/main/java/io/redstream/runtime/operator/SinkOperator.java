package io.redstream.runtime.operator;

import io.redstream.api.connector.RedisStreamSink;
import io.redstream.redis.OutRecord;

/** Appends to the outbox addressed to an external sink stream. */
final class SinkOperator<T> implements Operator<T> {

    private final RedisStreamSink<T> sink;
    private TaskContext ctx;

    SinkOperator(RedisStreamSink<T> sink) {
        this.sink = sink;
    }

    @Override
    public void open(TaskContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void process(T value, long timestamp) {
        ctx.emit(new OutRecord(sink.stream(), sink.serde().serialize(value), timestamp));
    }
}

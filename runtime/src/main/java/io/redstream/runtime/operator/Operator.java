package io.redstream.runtime.operator;

/**
 * A chained operator. Records move between operators in a stage by direct method call, the
 * same as Flink's chained operators: no serialization, no queue.
 */
public interface Operator<I> {

    default void open(TaskContext ctx) throws Exception {}

    void process(I value, long timestamp) throws Exception;
}

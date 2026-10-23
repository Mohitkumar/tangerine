package io.redstream.runtime.checkpoint;

/**
 * A marker that flows through the data, like Flink's {@code CheckpointBarrier}. Everything a
 * task processed before barrier {@code n} belongs to checkpoint {@code n}; everything after it
 * belongs to the next one.
 *
 * <p>Sources inject barriers when the coordinator requests a checkpoint, and every task
 * forwards them to all of its outputs, even outputs it sent no data to. On the wire a barrier is
 * a stream entry in the same stream as the data (plan step 5). Hint: add a kind field to the
 * entry fields in {@code MetaFields} so a reader can tell a barrier from a record.
 *
 * @param checkpointId       increases by one for every checkpoint the coordinator triggers
 * @param triggeredAtMillis  coordinator time when the checkpoint was triggered
 */
public record CheckpointBarrier(long checkpointId, long triggeredAtMillis) {

    public CheckpointBarrier {
        if (checkpointId <= 0) {
            throw new IllegalArgumentException("checkpoint ids start at 1");
        }
    }
}

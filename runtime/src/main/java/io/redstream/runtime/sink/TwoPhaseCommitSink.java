package io.redstream.runtime.sink;

import io.redstream.api.serde.Serde;

/**
 * A sink whose output becomes visible only when a checkpoint completes, like Flink's
 * {@code TwoPhaseCommitSinkFunction}. This gives exactly-once output without consumers having
 * to drop duplicates.
 *
 * <p>Lifecycle per checkpoint:
 *
 * <ol>
 *   <li>{@link #beginTransaction()} after the previous barrier;</li>
 *   <li>{@link #write} for each record;</li>
 *   <li>{@link #preCommit} at the barrier: make the data durable but still invisible. The
 *       transaction goes into the task's snapshot (serialized with {@link #transactionSerde()});</li>
 *   <li>{@link #commit} when the checkpoint completes: make it visible.</li>
 * </ol>
 *
 * <p>After a crash the restored task calls {@link #commit} again for transactions in its
 * snapshot, because the crash may have happened before the commit, so commit must be
 * idempotent. Plan step 7.
 *
 * @param <T>   record type
 * @param <TXN> transaction handle, for example the name of a staging stream
 */
public interface TwoPhaseCommitSink<T, TXN> {

    TXN beginTransaction() throws Exception;

    void write(TXN transaction, T value, long timestamp) throws Exception;

    void preCommit(TXN transaction) throws Exception;

    /** Makes the transaction's data visible. Must be safe to call more than once. */
    void commit(TXN transaction);

    /** Throws the transaction's data away, for example after an aborted checkpoint or a restart. */
    void abort(TXN transaction);

    Serde<TXN> transactionSerde();
}

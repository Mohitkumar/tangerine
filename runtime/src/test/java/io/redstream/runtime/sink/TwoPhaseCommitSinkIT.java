package io.redstream.runtime.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.redstream.redis.MetaFields;
import io.redstream.runtime.Impl;
import io.redstream.runtime.RedisTestBase;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The two-phase-commit sink on its own, without a job (plan step 7). */
class TwoPhaseCommitSinkIT extends RedisTestBase {

    private static List<String> visible(String stream) {
        return ops.xread(stream, "0-0", 10_000).stream()
                .map(e -> new String(e.field(MetaFields.F_VALUE), StandardCharsets.UTF_8))
                .toList();
    }

    private interface Scenario {
        <TXN> void run(TwoPhaseCommitSink<String, TXN> sink, String stream) throws Exception;
    }

    private static void with(Scenario s) throws Exception {
        String job = newJob("sink");
        String stream = job + ":out";
        s.run(Impl.redisStreamSink(ops, job, stream), stream);
    }

    @Test
    void step7_nothingIsVisibleBeforeCommit() throws Exception {
        with(new Scenario() {
            public <TXN> void run(TwoPhaseCommitSink<String, TXN> sink, String stream) throws Exception {
                TXN t = sink.beginTransaction();
                sink.write(t, "a", 1);
                sink.write(t, "b", 2);
                assertEquals(List.of(), visible(stream), "before preCommit");
                sink.preCommit(t);
                assertEquals(List.of(), visible(stream), "after preCommit, before commit");
                sink.commit(t);
                assertEquals(List.of("a", "b"), visible(stream));
            }
        });
    }

    @Test
    void step7_committingTwiceWritesOnce() throws Exception {
        with(new Scenario() {
            public <TXN> void run(TwoPhaseCommitSink<String, TXN> sink, String stream) throws Exception {
                TXN t = sink.beginTransaction();
                sink.write(t, "a", 1);
                sink.preCommit(t);
                sink.commit(t);
                sink.commit(t);
                assertEquals(List.of("a"), visible(stream));
            }
        });
    }

    @Test
    void step7_abortThrowsTheDataAway() throws Exception {
        with(new Scenario() {
            public <TXN> void run(TwoPhaseCommitSink<String, TXN> sink, String stream) throws Exception {
                TXN t = sink.beginTransaction();
                sink.write(t, "lost", 1);
                sink.preCommit(t);
                sink.abort(t);
                assertEquals(List.of(), visible(stream));
            }
        });
    }

    @Test
    void step7_aRestoredTransactionCanBeCommitted() throws Exception {
        with(new Scenario() {
            public <TXN> void run(TwoPhaseCommitSink<String, TXN> sink, String stream) throws Exception {
                TXN t = sink.beginTransaction();
                sink.write(t, "a", 1);
                sink.preCommit(t);
                byte[] inSnapshot = sink.transactionSerde().serialize(t);

                // the task crashes here; a new owner restores the transaction from the snapshot
                TXN restored = sink.transactionSerde().deserialize(inSnapshot);
                sink.commit(restored);
                sink.commit(restored);
                assertEquals(List.of("a"), visible(stream));
            }
        });
    }

    @Test
    void step7_transactionsCommitInOrder() throws Exception {
        with(new Scenario() {
            public <TXN> void run(TwoPhaseCommitSink<String, TXN> sink, String stream) throws Exception {
                TXN first = sink.beginTransaction();
                sink.write(first, "1a", 1);
                sink.preCommit(first);
                TXN second = sink.beginTransaction();
                sink.write(second, "2a", 2);
                sink.preCommit(second);
                sink.commit(first);
                sink.commit(second);
                assertEquals(List.of("1a", "2a"), visible(stream));
            }
        });
    }
}

package io.redstream.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.redstream.api.CheckpointConfig;
import io.redstream.api.JobDefinition;
import io.redstream.api.StreamEnvironment;
import io.redstream.api.connector.RedisStreamSink;
import io.redstream.api.connector.RedisStreamSource;
import io.redstream.api.functions.Collector;
import io.redstream.api.functions.KeyedProcessFunction;
import io.redstream.api.functions.RuntimeContext;
import io.redstream.api.serde.Serdes;
import io.redstream.api.state.ValueState;
import io.redstream.api.state.ValueStateDescriptor;
import io.redstream.api.time.WatermarkStrategy;
import io.redstream.redis.LettuceRedisOps;
import io.redstream.redis.MetaFields;
import io.redstream.runtime.checkpoint.CheckpointMetadata;
import io.redstream.runtime.checkpoint.CheckpointStorage;
import io.redstream.runtime.fault.FaultHooks;
import io.redstream.runtime.fault.RandomFaults;
import io.redstream.testkit.OutputChecker;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Whole jobs on real workers: the tests that say a step really works. Sink entries are read
 * from field {@code v}. Word count emits {@code word<TAB>count} on every update, so the highest
 * count seen per word is the job's state for that word.
 *
 * <p>Crashes come from {@link RandomFaults} at the {@code CrashPoint}s your {@code StreamTask}
 * reports, so call {@code FaultHooks.at(...)} at each of them.
 */
class EndToEndIT extends RedisTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final CheckpointConfig EVERY_200MS = new CheckpointConfig(Duration.ofMillis(200), Duration.ofSeconds(10), Duration.ZERO);
    private static final String[] VOCAB = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu".split(" ");

    @TempDir
    Path tmp;

    private final List<Worker> workers = new ArrayList<>();

    @AfterEach
    void stopWorkers() {
        workers.forEach(Worker::close);
    }

    static final class Counter extends KeyedProcessFunction<String, String, String> {
        private ValueState<Long> count;

        @Override
        public void open(RuntimeContext ctx) {
            count = ctx.getState(new ValueStateDescriptor<>("count", Serdes.longs()));
        }

        @Override
        public void processElement(String word, Context<String> ctx, Collector<String> out) {
            long next = (count.value() == null ? 0 : count.value()) + 1;
            count.update(next);
            out.collect(word + "\t" + next);
        }
    }

    /** A word count job plus the input it was fed, so tests can compare. */
    private static final class WordCount {
        final String name = newJob("e2e");
        final String in0 = name + ":in:0";
        final String in1 = name + ":in:1";
        final String sink = name + ":out";
        final Map<String, Long> expected = new TreeMap<>();
        final Random rnd = new Random(7);
        long words;
        final JobDefinition def;

        WordCount(boolean checkpointing) {
            var env = StreamEnvironment.create().maxParallelism(8);
            if (checkpointing) {
                Impl.enableCheckpointing(env, EVERY_200MS);
            }
            env.fromSource(RedisStreamSource.of(Serdes.string(), in0, in1), WatermarkStrategy.noWatermarks(), "lines")
                    .flatMap("split", (String line, Collector<String> out) -> {
                        for (String w : line.split(" ")) {
                            if (!w.isEmpty()) {
                                out.collect(w);
                            }
                        }
                    }, Serdes.string())
                    .keyBy(w -> w, Serdes.string())
                    .process("count", Counter::new, Serdes.string())
                    .sinkTo(RedisStreamSink.of(sink, Serdes.string()));
            def = env.build(name);
        }

        void feed(int lines) {
            for (int i = 0; i < lines; i++) {
                var line = new StringBuilder();
                for (int j = 0, n = 1 + rnd.nextInt(6); j < n; j++) {
                    String w = VOCAB[rnd.nextInt(VOCAB.length)];
                    line.append(w).append(' ');
                    expected.merge(w, 1L, Long::sum);
                    words++;
                }
                ops.xadd(i % 2 == 0 ? in0 : in1, Map.of("v", utf8(line.toString())));
            }
        }

        List<String> sinkValues() {
            return EndToEndIT.sinkValues(sink);
        }

        /** Highest count per word in the sink; fails at once if any word was counted too often. */
        Map<String, Long> maxCounts() {
            Map<String, Long> max = new TreeMap<>();
            for (String v : sinkValues()) {
                String[] p = v.split("\t");
                max.merge(p[0], Long.parseLong(p[1]), Math::max);
            }
            max.forEach((w, c) -> {
                if (c > expected.getOrDefault(w, 0L)) {
                    fail("'" + w + "' counted " + c + " times but was fed " + expected.getOrDefault(w, 0L)
                            + ": state was updated twice for some records");
                }
            });
            return max;
        }

        void awaitCorrectCounts() {
            await("final word counts", () -> maxCounts().equals(expected));
            assertEquals("", OutputChecker.diff(expected, maxCounts()));
        }
    }

    private Worker start(JobDefinition def, String id, FaultHooks hooks, WorkerConfig base) {
        Worker w = Worker.start(def, base.withWorkerId(id), LettuceRedisOps.standalone(redis.uri()), hooks);
        workers.add(w);
        return w;
    }

    private static WorkerConfig config() {
        return WorkerConfig.defaults(redis.uri()).withBatchSize(20).withLeaseTtl(Duration.ofMillis(600));
    }

    private static List<String> sinkValues(String stream) {
        return ops.xread(stream, "0-0", 10_000_000).stream()
                .map(e -> new String(e.field(MetaFields.F_VALUE), StandardCharsets.UTF_8))
                .toList();
    }

    private static void await(String what, BooleanSupplier done) {
        long deadline = System.currentTimeMillis() + TIMEOUT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (done.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }
        fail("timed out after " + TIMEOUT.toSeconds() + "s waiting for " + what);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Workers a and b with crashes, a killed, c joining, with input fed between each change. */
    private void crashAndRebalance(WordCount job, WorkerConfig cfg) throws Exception {
        var faults = new RandomFaults(42, 0.02);
        Worker a = start(job.def, "a", faults, cfg);
        start(job.def, "b", faults, cfg);
        job.feed(300);
        Thread.sleep(1_500);
        a.close();
        workers.remove(a);
        job.feed(300);
        start(job.def, "c", faults, cfg);
        job.feed(300);
    }

    // ---- Step 1 -------------------------------------------------------------------------

    @Test
    void step1_wordCountIsCorrectWithoutFailures() {
        var job = new WordCount(false);
        start(job.def, "solo", FaultHooks.NONE, config());
        job.feed(300);
        job.awaitCorrectCounts();
        assertEquals(job.words, job.sinkValues().size(), "no failures, so one sink entry per input word");
    }

    // ---- Step 4 -------------------------------------------------------------------------

    @Test
    void step4_sourceOnlyJobLosesNothingUnderCrashes() {
        String name = newJob("e2e-src");
        String in0 = name + ":in:0", in1 = name + ":in:1", sink = name + ":out";
        var env = StreamEnvironment.create().maxParallelism(8);
        Impl.enableCheckpointing(env, EVERY_200MS);
        env.fromSource(RedisStreamSource.of(Serdes.string(), in0, in1), WatermarkStrategy.noWatermarks(), "lines")
                .map("upper", (String s) -> s.toUpperCase(), Serdes.string())
                .sinkTo(RedisStreamSink.of(sink, Serdes.string()));
        JobDefinition def = env.build(name);
        CheckpointStorage storage = Impl.redisCheckpointStorage(ops, name);

        var faults = new RandomFaults(11, 0.02);
        start(def, "a", faults, config());
        start(def, "b", faults, config());
        Set<String> fed = new HashSet<>();
        for (int i = 0; i < 600; i++) {
            ops.xadd(i % 2 == 0 ? in0 : in1, Map.of("v", utf8("line-" + i)));
            fed.add("LINE-" + i);
        }

        await("every input line in the sink", () -> new HashSet<>(sinkValues(sink)).containsAll(fed));
        await("a completed checkpoint", () -> storage.latestCompleted().isPresent());

        CheckpointMetadata latest = storage.latestCompleted().orElseThrow();
        assertEquals(2, latest.tasks().size(), "one snapshot per source partition");
        latest.tasks().forEach(t -> assertTrue(t.sourceOffset() != null, "source " + t.partition() + " recorded its offset"));
    }

    // ---- Step 5 -------------------------------------------------------------------------

    @Test
    void step5_keyedStateIsExactUnderCrashesAndRebalancing() throws Exception {
        var job = new WordCount(true);
        crashAndRebalance(job, config());
        job.awaitCorrectCounts();
    }

    // ---- Step 6 -------------------------------------------------------------------------

    @Test
    void step6_keyedStateIsExactWithRocksDb() throws Exception {
        WorkerConfig cfg = Impl.withRocksDb(config(), tmp);
        var job = new WordCount(true);
        crashAndRebalance(job, cfg);
        job.awaitCorrectCounts();
    }

    // ---- Step 7 -------------------------------------------------------------------------

    @Test
    void step7_sinkOutputIsExactlyOnce() throws Exception {
        Impl.sinksAreTransactional();
        var job = new WordCount(true);
        crashAndRebalance(job, config());
        await("one sink entry per input word", () -> job.sinkValues().size() >= job.words);
        job.awaitCorrectCounts();

        List<String> values = job.sinkValues();
        assertEquals(job.words, values.size(), "no duplicates, no consumer-side dedup");
        Map<String, Integer> times = new HashMap<>();
        values.forEach(v -> times.merge(v, 1, Integer::sum));
        times.forEach((v, n) -> assertEquals(1, n, "'" + v.replace('\t', '=') + "' appears once"));
    }

    // ---- Step 8 -------------------------------------------------------------------------

    @Test
    void step8_savepointThenRescaleKeepsTheCounts() throws Exception {
        var job = new WordCount(true);
        CheckpointStorage storage = Impl.redisCheckpointStorage(ops, job.name);
        start(job.def, "a", FaultHooks.NONE, config());
        start(job.def, "b", FaultHooks.NONE, config());
        job.feed(300);
        job.awaitCorrectCounts();

        Impl.triggerSavepoint(ops, job.name);
        await("a completed savepoint", () -> storage.latestCompleted().map(CheckpointMetadata::savepoint).orElse(false));
        Optional<CheckpointMetadata> savepoint = storage.latestCompleted();
        workers.forEach(Worker::close);
        workers.clear();

        for (String id : List.of("x", "y", "z")) {
            start(job.def, id, FaultHooks.NONE, config());
        }
        job.feed(300);
        job.awaitCorrectCounts();
        assertTrue(storage.load(savepoint.orElseThrow().checkpointId()).isPresent(), "savepoints are never discarded");
    }
}

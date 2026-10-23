package io.redstream.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import io.redstream.redis.RedisOps;
import io.redstream.redis.StreamEntry;
import io.redstream.runtime.fault.RandomFaults;
import io.redstream.testkit.OutputChecker;
import io.redstream.testkit.TestRedis;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Word count under random crashes at every crash point, plus a worker killed mid-run and a
 * new one joining. Final per-word counts must equal an in-memory reference, and after
 * dropping duplicate (producer, seq) pairs the sink must hold exactly one entry per input word.
 */
@Tag("integration")
class ExactlyOnceIT {

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

    @Test
    void countsSurviveCrashesAndRebalancing() throws Exception {
        String job = "eo-" + UUID.randomUUID().toString().substring(0, 8);
        String in0 = job + ":in:0", in1 = job + ":in:1", sink = job + ":out";
        var env = StreamEnvironment.create().maxParallelism(16);
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
        JobDefinition def = env.build(job);

        try (var redis = TestRedis.start(); RedisOps admin = LettuceRedisOps.standalone(redis.uri())) {
            String[] vocab = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu".split(" ");
            var rnd = new Random(7);
            Map<String, Long> expected = new TreeMap<>();
            long[] total = {0};
            Runnable feed = () -> {
                for (int i = 0; i < 400; i++) {
                    var line = new StringBuilder();
                    for (int j = 0, n = 1 + rnd.nextInt(6); j < n; j++) {
                        String w = vocab[rnd.nextInt(vocab.length)];
                        line.append(w).append(' ');
                        expected.merge(w, 1L, Long::sum);
                        total[0]++;
                    }
                    admin.xadd(i % 2 == 0 ? in0 : in1, Map.of("v", line.toString().getBytes(StandardCharsets.UTF_8)));
                }
            };

            var cfg = WorkerConfig.defaults(redis.uri()).withBatchSize(20).withLeaseTtl(Duration.ofMillis(600));
            var faults = new RandomFaults(42, 0.05);
            var a = Worker.start(def, cfg.withWorkerId("a"), LettuceRedisOps.standalone(redis.uri()), faults);
            var b = Worker.start(def, cfg.withWorkerId("b"), LettuceRedisOps.standalone(redis.uri()), faults);
            feed.run();
            Thread.sleep(1500);
            a.close();
            feed.run();
            var c = Worker.start(def, cfg.withWorkerId("c"), LettuceRedisOps.standalone(redis.uri()), faults);
            feed.run();

            Map<String, Long> actual = new TreeMap<>();
            int distinct = 0;
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                Map<String, String> byProducerSeq = new HashMap<>();
                for (StreamEntry e : admin.xread(sink, "0-0", 1_000_000)) {
                    byProducerSeq.put(utf8(e.field(MetaFields.F_PRODUCER)) + "/" + utf8(e.field(MetaFields.F_SEQ)),
                            utf8(e.field(MetaFields.F_VALUE)));
                }
                actual.clear();
                byProducerSeq.values().forEach(v -> {
                    String[] p = v.split("\t");
                    actual.merge(p[0], Long.parseLong(p[1]), Math::max);
                });
                distinct = byProducerSeq.size();
                if (distinct >= total[0]) {
                    break;
                }
                Thread.sleep(250);
            }
            b.close();
            c.close();

            assertEquals("", OutputChecker.diff(expected, actual));
            assertEquals(total[0], distinct, "one sink entry per input word after (producer, seq) dedup");
        }
    }

    private static String utf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }
}

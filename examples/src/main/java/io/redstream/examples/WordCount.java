package io.redstream.examples;

import io.redstream.api.JobDefinition;
import io.redstream.api.StreamEnvironment;
import io.redstream.api.connector.RedisStreamSink;
import io.redstream.api.connector.RedisStreamSource;
import io.redstream.api.functions.Collector;
import io.redstream.api.functions.KeyedProcessFunction;
import io.redstream.api.functions.RuntimeContext;
import io.redstream.api.serde.Serde;
import io.redstream.api.serde.Serdes;
import io.redstream.api.state.ValueState;
import io.redstream.api.state.ValueStateDescriptor;
import io.redstream.api.time.WatermarkStrategy;
import io.redstream.runtime.Worker;
import io.redstream.runtime.WorkerConfig;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

/**
 * Running word count. Reads lines from {@code wc:lines:0} and {@code wc:lines:1} (field {@code v})
 * and appends {@code word<TAB>count} to {@code wc:counts} on every update.
 *
 * <pre>
 *   redis-cli XADD wc:lines:0 '*' v "the quick brown fox"
 *   ./gradlew :examples:run                # start one or more workers
 *   redis-cli XRANGE wc:counts - +
 * </pre>
 */
public final class WordCount {

    public record Count(String word, long count) {
        public static final Serde<Count> SERDE = Serdes.of(
                c -> (c.word + "\t" + c.count).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                b -> {
                    String[] parts = new String(b, java.nio.charset.StandardCharsets.UTF_8).split("\t", 2);
                    return new Count(parts[0], Long.parseLong(parts[1]));
                });
    }

    /** Keeps a running count per word in keyed state. */
    static final class CountWords extends KeyedProcessFunction<String, String, Count> {
        private ValueState<Long> count;

        @Override
        public void open(RuntimeContext ctx) {
            count = ctx.getState(new ValueStateDescriptor<>("count", Serdes.longs()));
        }

        @Override
        public void processElement(String word, Context<String> ctx, Collector<Count> out) {
            Long current = count.value();
            long next = (current == null ? 0 : current) + 1;
            count.update(next);
            out.collect(new Count(word, next));
        }
    }

    public static JobDefinition job() {
        var env = StreamEnvironment.create().maxParallelism(16);
        env.fromSource(RedisStreamSource.of(Serdes.string(), "wc:lines:0", "wc:lines:1"), WatermarkStrategy.noWatermarks(), "lines")
                .flatMap("split", (String line, Collector<String> out) -> {
                    for (String w : line.toLowerCase(Locale.ROOT).split("\\W+")) {
                        if (!w.isEmpty()) {
                            out.collect(w);
                        }
                    }
                }, Serdes.string())
                .keyBy(w -> w, Serdes.string())
                .process("count", CountWords::new, Count.SERDE)
                .sinkTo(RedisStreamSink.of("wc:counts", Count.SERDE));
        return env.build("wordcount");
    }

    public static void main(String[] args) throws InterruptedException {
        Worker worker = Worker.start(job(), WorkerConfig.fromEnv());
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.close();
            stopped.countDown();
        }));
        stopped.await();
    }
}

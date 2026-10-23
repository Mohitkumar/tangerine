package io.redstream.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import io.redstream.plan.JobGraph;
import io.redstream.plan.LogicalPlanner;
import io.redstream.redis.KeySpace;
import io.redstream.redis.OutRecord;
import io.redstream.runtime.operator.OperatorChain;
import io.redstream.runtime.operator.TaskContext;
import io.redstream.runtime.state.KeyedStateStore;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Runs stages in memory, without Redis. */
class OperatorChainTest {

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
            out.collect(word + "=" + next);
        }
    }

    private static JobGraph job(Supplier<? extends KeyedProcessFunction<String, String, String>> factory) {
        var env = StreamEnvironment.create().maxParallelism(8);
        env.fromSource(RedisStreamSource.of(Serdes.string(), "lines"), WatermarkStrategy.noWatermarks(), "lines")
                .flatMap("split", (String line, Collector<String> out) -> {
                    for (String w : line.split(" ")) {
                        out.collect(w);
                    }
                }, Serdes.string())
                .keyBy(w -> w, Serdes.string())
                .process("count", factory, Serdes.string())
                .sinkTo(RedisStreamSink.of("counts", Serdes.string()));
        return LogicalPlanner.plan(env.build("chain-test"));
    }

    @Test
    void sourceStageShufflesToKeyGroupInputs() throws Exception {
        JobGraph graph = job(Counter::new);
        var ctx = new TaskContext("chain-test", new KeySpace("chain-test"), 8, null);
        var chain = OperatorChain.build(graph.stage(0));
        chain.open(ctx);
        chain.process("a b a", 1L);
        assertEquals(3, ctx.outputs().size());
        String dest = ctx.outputs().get(0).destination();
        assertTrue(dest.startsWith("{chain-test:s1:p") && dest.endsWith("}:in"), dest);
        assertEquals(dest, ctx.outputs().get(2).destination(), "same key, same key group");
    }

    @Test
    void keyedStageKeepsSeparateStatePerKey() throws Exception {
        JobGraph graph = job(Counter::new);
        var ctx = new TaskContext("chain-test", new KeySpace("chain-test"), 8, new KeyedStateStore(Map.of()));
        var chain = OperatorChain.build(graph.stage(1));
        chain.open(ctx);
        for (String w : List.of("mu", "lambda", "mu", "mu", "lambda")) {
            chain.process(w, 1L);
        }
        List<String> out = ctx.outputs().stream().map(OutRecord::payload).map(b -> new String(b, StandardCharsets.UTF_8)).toList();
        assertEquals(List.of("mu=1", "lambda=1", "mu=2", "mu=3", "lambda=2"), out);
        assertEquals("counts", ctx.outputs().get(0).destination());
    }

    @Test
    void rejectsSupplierThatReturnsSharedInstance() {
        Counter shared = new Counter();
        JobGraph graph = job(() -> shared);
        OperatorChain.build(graph.stage(1));
        var e = assertThrows(IllegalStateException.class, () -> OperatorChain.build(graph.stage(1)));
        assertTrue(e.getMessage().contains("new instance"));
    }
}

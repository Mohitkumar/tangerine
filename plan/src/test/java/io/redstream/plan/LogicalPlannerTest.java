package io.redstream.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.api.StreamEnvironment;
import io.redstream.api.connector.RedisStreamSink;
import io.redstream.api.connector.RedisStreamSource;
import io.redstream.api.functions.Collector;
import io.redstream.api.serde.Serdes;
import io.redstream.api.time.WatermarkStrategy;
import io.redstream.api.window.TumblingEventTimeWindows;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class LogicalPlannerTest {

    @Test
    void keyBySplitsStagesAndChainsTheRest() {
        var env = StreamEnvironment.create();
        env.fromSource(RedisStreamSource.of(Serdes.string(), "in:0", "in:1"), WatermarkStrategy.noWatermarks(), "lines")
                .flatMap("split", (String line, Collector<String> out) -> {
                    for (String w : line.split(" ")) {
                        out.collect(w);
                    }
                }, Serdes.string())
                .filter("nonEmpty", w -> !w.isEmpty())
                .keyBy(w -> w, Serdes.string())
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .reduce("concat", (a, b) -> a + b)
                .sinkTo(RedisStreamSink.of("out", Serdes.string()));

        JobGraph graph = LogicalPlanner.plan(env.build("wc"));

        assertEquals(2, graph.stages().size());
        Stage source = graph.stage(0);
        Stage keyed = graph.stage(1);
        assertFalse(source.isKeyed());
        assertEquals(2, source.partitionCount(128));
        assertEquals(List.of("split", "nonEmpty"), source.operators().stream().map(o -> o.name()).toList());
        assertEquals(1, source.shuffleOutputs().size());
        assertEquals(1, source.shuffleOutputs().get(0).targetStageId());
        assertTrue(keyed.isKeyed());
        assertEquals(128, keyed.partitionCount(128));
        assertEquals(List.of("concat"), keyed.operators().stream().map(o -> o.name()).toList());
        assertEquals(1, keyed.sinks().size());
    }

    @Test
    void rejectsDanglingStage() {
        var env = StreamEnvironment.create();
        env.fromSource(RedisStreamSource.of(Serdes.string(), "in"), WatermarkStrategy.noWatermarks(), "src")
                .map(String::length, Serdes.ints());
        var e = assertThrows(IllegalStateException.class, () -> LogicalPlanner.plan(env.build("dangling")));
        assertTrue(e.getMessage().contains("no sink"));
    }

    @Test
    void rejectsJobNamesThatWouldBreakHashTags() {
        var env = StreamEnvironment.create();
        env.fromSource(RedisStreamSource.of(Serdes.string(), "in"), WatermarkStrategy.noWatermarks(), "src")
                .sinkTo(RedisStreamSink.of("out", Serdes.string()));
        assertThrows(IllegalArgumentException.class, () -> env.build("bad{name}"));
        assertThrows(IllegalArgumentException.class, () -> env.build("Has:Colon"));
    }
}

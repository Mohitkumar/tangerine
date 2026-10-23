package io.redstream.plan;

import io.redstream.api.JobDefinition;
import io.redstream.api.transformation.OneInputTransformation;
import io.redstream.api.transformation.PartitionTransformation;
import io.redstream.api.transformation.SinkTransformation;
import io.redstream.api.transformation.SourceTransformation;
import io.redstream.api.transformation.Transformation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns the transformation list into stages. Operator chaining rule: an operator joins its
 * input's stage unless the input is a {@link PartitionTransformation}, which starts a new
 * stage. Transformations are visited in id order, which is a topological order.
 */
public final class LogicalPlanner {

    private LogicalPlanner() {}

    public static JobGraph plan(JobDefinition job) {
        List<StageBuilder> stages = new ArrayList<>();
        Map<Integer, StageBuilder> stageOf = new HashMap<>();

        for (Transformation<?> t : job.transformations()) {
            switch (t) {
                case SourceTransformation<?> s -> {
                    var sb = new StageBuilder(stages.size(), new StageInput.FromSource(s));
                    stages.add(sb);
                    stageOf.put(s.id(), sb);
                }
                case PartitionTransformation<?, ?> p -> {
                    var upstream = require(stageOf, p.input());
                    var sb = new StageBuilder(stages.size(), new StageInput.FromShuffle(upstream.id, p));
                    stages.add(sb);
                    upstream.shuffleOutputs.add(new ShuffleOutput(sb.id, p));
                    stageOf.put(p.id(), sb);
                }
                case OneInputTransformation<?, ?> o -> {
                    if (o.isKeyed() && !(o.input() instanceof PartitionTransformation<?, ?>)) {
                        throw new IllegalStateException("keyed operator '" + o.name() + "' must directly follow keyBy");
                    }
                    var sb = require(stageOf, o.input());
                    sb.operators.add(o);
                    stageOf.put(o.id(), sb);
                }
                case SinkTransformation<?> s -> {
                    var sb = require(stageOf, s.input());
                    sb.sinks.add(s);
                    stageOf.put(s.id(), sb);
                }
            }
        }

        for (StageBuilder sb : stages) {
            if (sb.shuffleOutputs.isEmpty() && sb.sinks.isEmpty()) {
                throw new IllegalStateException("stage " + sb.id + " has no sink or keyBy output, so its results would be discarded");
            }
        }
        return new JobGraph(job.name(), job.maxParallelism(), stages.stream().map(StageBuilder::build).toList());
    }

    private static StageBuilder require(Map<Integer, StageBuilder> stageOf, Transformation<?> input) {
        var sb = stageOf.get(input.id());
        if (sb == null) {
            throw new IllegalStateException("input " + input.name() + " was not planned before its consumer");
        }
        return sb;
    }

    private static final class StageBuilder {
        final int id;
        final StageInput input;
        final List<OneInputTransformation<?, ?>> operators = new ArrayList<>();
        final List<ShuffleOutput> shuffleOutputs = new ArrayList<>();
        final List<SinkTransformation<?>> sinks = new ArrayList<>();

        StageBuilder(int id, StageInput input) {
            this.id = id;
            this.input = input;
        }

        Stage build() {
            return new Stage(id, input, operators, shuffleOutputs, sinks);
        }
    }
}

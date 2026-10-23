package io.redstream.plan;

import io.redstream.api.transformation.OneInputTransformation;
import io.redstream.api.transformation.SinkTransformation;
import java.util.List;

/**
 * A chain of operators that runs in one thread per partition, with records passed by method
 * call. Stages are separated by keyBy edges, which go through Redis. The equivalent of a
 * Flink JobGraph vertex.
 *
 * @param operators chained operators in topological order (ascending transformation id)
 */
public record Stage(int id, StageInput input, List<OneInputTransformation<?, ?>> operators,
                    List<ShuffleOutput> shuffleOutputs, List<SinkTransformation<?>> sinks) {

    public Stage {
        operators = List.copyOf(operators);
        shuffleOutputs = List.copyOf(shuffleOutputs);
        sinks = List.copyOf(sinks);
    }

    /** Number of partitions: source streams for a source stage, key groups for a keyed stage. */
    public int partitionCount(int maxParallelism) {
        return switch (input) {
            case StageInput.FromSource s -> s.source().source().streams().size();
            case StageInput.FromShuffle s -> maxParallelism;
        };
    }

    public boolean isKeyed() {
        return input instanceof StageInput.FromShuffle;
    }

    public String describe() {
        var sb = new StringBuilder("stage ").append(id).append(": ");
        sb.append(switch (input) {
            case StageInput.FromSource s -> "source[" + s.source().name() + "]";
            case StageInput.FromShuffle s -> "shuffle(from stage " + s.upstreamStageId() + ")";
        });
        operators.forEach(o -> sb.append(" -> ").append(o.name()));
        shuffleOutputs.forEach(o -> sb.append(" -> keyBy(to stage ").append(o.targetStageId()).append(')'));
        sinks.forEach(s -> sb.append(" -> ").append(s.name()));
        return sb.toString();
    }
}

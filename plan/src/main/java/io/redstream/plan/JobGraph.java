package io.redstream.plan;

import java.util.List;
import java.util.stream.Collectors;

/** The planned job: stages in topological order. Every worker computes the identical graph. */
public record JobGraph(String jobName, int maxParallelism, List<Stage> stages) {

    public JobGraph {
        stages = List.copyOf(stages);
    }

    public Stage stage(int id) {
        return stages.get(id);
    }

    public String describe() {
        return stages.stream().map(Stage::describe).collect(Collectors.joining("\n"));
    }
}

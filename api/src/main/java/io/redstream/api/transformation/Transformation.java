package io.redstream.api.transformation;

import java.util.List;

/**
 * One node of the logical job graph, recorded lazily by the DataStream API. Nothing executes
 * when these are created; the planner turns them into stages.
 *
 * <p>IDs increase in creation order, so a transformation's inputs always have smaller IDs.
 */
public sealed interface Transformation<T>
        permits SourceTransformation, OneInputTransformation, PartitionTransformation, SinkTransformation {

    int id();

    String name();

    List<Transformation<?>> inputs();
}

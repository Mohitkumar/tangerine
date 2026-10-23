package io.redstream.plan;

import io.redstream.api.transformation.PartitionTransformation;

/** A hash-partitioned edge leaving a stage for {@code targetStageId}. */
public record ShuffleOutput(int targetStageId, PartitionTransformation<?, ?> partition) {}

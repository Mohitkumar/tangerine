package io.redstream.plan;

import io.redstream.api.transformation.PartitionTransformation;
import io.redstream.api.transformation.SourceTransformation;

/** Where a stage's records come from. */
public sealed interface StageInput {

    /** Reads external Redis streams; one partition per source stream. */
    record FromSource(SourceTransformation<?> source) implements StageInput {}

    /** Reads its own per-key-group input streams, filled by an upstream stage's shuffle. */
    record FromShuffle(int upstreamStageId, PartitionTransformation<?, ?> partition) implements StageInput {}
}

package io.redstream.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Everything one batch changed, applied by a single {@code commit.lua} call:
 * state writes and deletes, outputs appended to the outbox, the advanced input offset,
 * the highest sequence seen per upstream producer, and the watermark.
 */
public record CommitBatch(String lastInputId, long watermark, Map<Bytes, byte[]> statePuts, Set<Bytes> stateDeletes,
                          List<OutRecord> outputs, Map<String, Long> seenSeqs) {

    public CommitBatch {
        statePuts = Map.copyOf(statePuts);
        stateDeletes = Set.copyOf(stateDeletes);
        outputs = List.copyOf(outputs);
        seenSeqs = Map.copyOf(seenSeqs);
    }
}

package io.redstream.redis;

import java.util.Map;

/** A partition's committed state as loaded from Redis when a task starts. */
public record PartitionSnapshot(long epoch, String inOffset, long outSeq, String fwdOffset, long watermark,
                                Map<String, Long> seenSeqs, Map<Bytes, byte[]> state) {}

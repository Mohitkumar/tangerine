package io.redstream.coord;

/** A unit of ownership: one partition of one stage. */
public record PartitionId(int stage, int partition) {}

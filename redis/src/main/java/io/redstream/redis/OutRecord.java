package io.redstream.redis;

/** A record headed out of a partition: to a downstream key group's input stream or to a sink stream. */
public record OutRecord(String destination, byte[] payload, long timestamp) {}

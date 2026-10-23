package io.redstream.redis;

/** Field names shared with the Lua scripts and between writers and readers of streams. */
public final class MetaFields {

    /** In a partition's {@code :meta} hash: bumped by {@code lease.lua} on every new owner. */
    public static final String EPOCH = "epoch";

    /**
     * Fields on every entry in a shuffled input stream or a sink stream. Plan step 5 adds a kind
     * field here, so a reader can tell a record from a checkpoint barrier.
     */
    public static final String F_TS = "ts";
    public static final String F_PRODUCER = "p";
    public static final String F_VALUE = "v";

    private MetaFields() {}
}

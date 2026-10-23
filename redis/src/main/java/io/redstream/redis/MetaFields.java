package io.redstream.redis;

/** Field names in a partition's {@code :meta} hash. Must match the Lua scripts. */
public final class MetaFields {

    public static final String EPOCH = "epoch";
    public static final String IN_OFFSET = "in_offset";
    public static final String OUT_SEQ = "out_seq";
    public static final String FWD_OFFSET = "fwd_offset";
    public static final String WATERMARK = "wm";
    public static final String SEEN_PREFIX = "seen:";

    /** Fields on every entry in an outbox, a shuffled input stream, or a sink stream. */
    public static final String F_DEST = "dst";
    public static final String F_TS = "ts";
    public static final String F_PRODUCER = "p";
    public static final String F_SEQ = "s";
    public static final String F_VALUE = "v";

    private MetaFields() {}
}

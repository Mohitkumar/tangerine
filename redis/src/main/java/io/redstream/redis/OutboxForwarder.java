package io.redstream.redis;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Moves committed records from a partition's outbox to their destinations. Destinations are
 * in other hash slots, so this step cannot be atomic with the commit; instead it is
 * idempotent. If it crashes after XADD but before the ack, the entries are forwarded again
 * with the same (producer, seq), and the downstream partition drops them.
 */
public final class OutboxForwarder {

    private final RedisOps ops;
    private final CommitProtocol protocol;

    public OutboxForwarder(RedisOps ops, CommitProtocol protocol) {
        this.ops = ops;
        this.protocol = protocol;
    }

    /**
     * Forwards up to {@code max} entries after {@code fromId}.
     *
     * @return the ID of the last forwarded entry, or {@code fromId} if nothing was pending
     */
    public String forwardOnce(PartitionKeys keys, long epoch, String fromId, int max) {
        return forwardOnce(keys, epoch, fromId, max, () -> {});
    }

    /** As above; {@code beforeAck} runs after the XADDs and before the ack (fault-injection point). */
    public String forwardOnce(PartitionKeys keys, long epoch, String fromId, int max, Runnable beforeAck) {
        List<StreamEntry> pending = ops.xread(keys.out(), fromId, max);
        if (pending.isEmpty()) {
            return fromId;
        }
        for (StreamEntry e : pending) {
            String destination = new String(e.field(MetaFields.F_DEST), java.nio.charset.StandardCharsets.UTF_8);
            Map<String, byte[]> fields = new LinkedHashMap<>();
            fields.put(MetaFields.F_PRODUCER, e.field(MetaFields.F_PRODUCER));
            fields.put(MetaFields.F_SEQ, e.field(MetaFields.F_SEQ));
            fields.put(MetaFields.F_TS, e.field(MetaFields.F_TS));
            fields.put(MetaFields.F_VALUE, e.field(MetaFields.F_VALUE));
            ops.xadd(destination, fields);
        }
        String last = pending.get(pending.size() - 1).id();
        beforeAck.run();
        protocol.ackForwarded(keys, epoch, last);
        return last;
    }
}

package io.redstream.redis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The engine's fault-tolerance core: restore a partition, commit a batch atomically, and
 * acknowledge forwarded outbox entries. Both writes are fenced by the partition epoch.
 */
public final class CommitProtocol {

    private static final LuaScript COMMIT = LuaScript.load(CommitProtocol.class, "/io/redstream/redis/lua/commit.lua");
    private static final LuaScript FORWARD_ACK = LuaScript.load(CommitProtocol.class, "/io/redstream/redis/lua/forward_ack.lua");

    private final RedisOps ops;

    public CommitProtocol(RedisOps ops) {
        this.ops = ops;
    }

    /** Loads meta and state. The caller must already hold the lease for {@code expectedEpoch}. */
    public PartitionSnapshot restore(PartitionKeys keys, String defaultInOffset) {
        Map<Bytes, byte[]> meta = ops.hgetall(keys.meta());
        Map<String, Long> seen = new HashMap<>();
        meta.forEach((k, v) -> {
            String field = k.toString();
            if (field.startsWith(MetaFields.SEEN_PREFIX)) {
                seen.put(field.substring(MetaFields.SEEN_PREFIX.length()), parseLong(v));
            }
        });
        return new PartitionSnapshot(
                longField(meta, MetaFields.EPOCH, 0),
                stringField(meta, MetaFields.IN_OFFSET, defaultInOffset),
                longField(meta, MetaFields.OUT_SEQ, 0),
                stringField(meta, MetaFields.FWD_OFFSET, "0-0"),
                longField(meta, MetaFields.WATERMARK, Long.MIN_VALUE),
                seen,
                ops.hgetall(keys.state()));
    }

    /**
     * Applies the batch atomically. Returns the producer sequence number of the last output.
     *
     * @throws FencedException if another worker has taken the partition
     */
    public long commit(PartitionKeys keys, long epoch, CommitBatch batch) {
        List<byte[]> args = new ArrayList<>();
        args.add(utf8(Long.toString(epoch)));
        args.add(utf8(batch.lastInputId()));
        args.add(utf8(Long.toString(batch.watermark())));
        args.add(utf8(keys.producerId()));
        args.add(utf8(Integer.toString(batch.statePuts().size())));
        batch.statePuts().forEach((k, v) -> {
            args.add(k.unsafeArray());
            args.add(v);
        });
        args.add(utf8(Integer.toString(batch.stateDeletes().size())));
        batch.stateDeletes().forEach(k -> args.add(k.unsafeArray()));
        args.add(utf8(Integer.toString(batch.outputs().size())));
        for (OutRecord r : batch.outputs()) {
            args.add(utf8(r.destination()));
            args.add(utf8(Long.toString(r.timestamp())));
            args.add(r.payload());
        }
        args.add(utf8(Integer.toString(batch.seenSeqs().size())));
        batch.seenSeqs().forEach((producer, seq) -> {
            args.add(utf8(producer));
            args.add(utf8(Long.toString(seq)));
        });
        return fenced(keys, () -> ops.evalLong(COMMIT, List.of(keys.meta(), keys.state(), keys.out()), args));
    }

    /** Records that outbox entries up to {@code lastForwardedId} were delivered, and trims them. */
    public void ackForwarded(PartitionKeys keys, long epoch, String lastForwardedId) {
        fenced(keys, () -> ops.evalLong(FORWARD_ACK, List.of(keys.meta(), keys.out()),
                List.of(utf8(Long.toString(epoch)), utf8(lastForwardedId))));
    }

    private static long fenced(PartitionKeys keys, java.util.function.LongSupplier call) {
        try {
            return call.getAsLong();
        } catch (RedisScriptException e) {
            if (e.getMessage() != null && e.getMessage().contains("FENCED")) {
                throw new FencedException(keys.tag(), e.getMessage());
            }
            throw e;
        }
    }

    private static long longField(Map<Bytes, byte[]> meta, String field, long dflt) {
        byte[] v = meta.get(Bytes.utf8(field));
        return v == null ? dflt : parseLong(v);
    }

    private static String stringField(Map<Bytes, byte[]> meta, String field, String dflt) {
        byte[] v = meta.get(Bytes.utf8(field));
        return v == null ? dflt : new String(v, StandardCharsets.UTF_8);
    }

    private static long parseLong(byte[] v) {
        return Long.parseLong(new String(v, StandardCharsets.UTF_8));
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

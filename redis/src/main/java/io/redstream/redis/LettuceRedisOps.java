package io.redstream.redis;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link RedisOps} on Lettuce with a byte-array codec. One multiplexed connection is shared by
 * all tasks; this is why reads never use XREAD BLOCK, which would stall every caller.
 */
public final class LettuceRedisOps implements RedisOps {

    private final AbstractRedisClient client;
    private final StatefulConnection<byte[], byte[]> connection;
    private final RedisClusterCommands<byte[], byte[]> commands;

    private LettuceRedisOps(AbstractRedisClient client, StatefulConnection<byte[], byte[]> connection,
                            RedisClusterCommands<byte[], byte[]> commands) {
        this.client = client;
        this.connection = connection;
        this.commands = commands;
    }

    /** Single Redis or Valkey node, e.g. {@code redis://localhost:6379}. */
    public static LettuceRedisOps standalone(String uri) {
        RedisClient client = RedisClient.create(uri);
        var conn = client.connect(ByteArrayCodec.INSTANCE);
        return new LettuceRedisOps(client, conn, conn.sync());
    }

    /** Redis Cluster; scripts are routed by their first key's hash slot. */
    public static LettuceRedisOps cluster(String uri) {
        RedisClusterClient client = RedisClusterClient.create(uri);
        var conn = client.connect(ByteArrayCodec.INSTANCE);
        return new LettuceRedisOps(client, conn, conn.sync());
    }

    @Override
    public long evalLong(LuaScript script, List<String> keys, List<byte[]> args) {
        Long result = eval(script, ScriptOutputType.INTEGER, keys, args);
        return result;
    }

    @Override
    public List<byte[]> evalList(LuaScript script, List<String> keys, List<byte[]> args) {
        List<byte[]> result = eval(script, ScriptOutputType.MULTI, keys, args);
        return result;
    }

    private <T> T eval(LuaScript script, ScriptOutputType type, List<String> keys, List<byte[]> args) {
        byte[][] k = keys.stream().map(LettuceRedisOps::utf8).toArray(byte[][]::new);
        byte[][] a = args.toArray(byte[][]::new);
        try {
            try {
                return commands.evalsha(script.sha1(), type, k, a);
            } catch (RedisCommandExecutionException e) {
                if (e.getMessage() == null || !e.getMessage().startsWith("NOSCRIPT")) {
                    throw e;
                }
                return commands.eval(script.source(), type, k, a);
            }
        } catch (RedisCommandExecutionException e) {
            throw new RedisScriptException(script.name() + ": " + e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<StreamEntry> xread(String stream, String afterId, int count) {
        List<StreamMessage<byte[], byte[]>> messages =
                commands.xread(XReadArgs.Builder.count(count), XReadArgs.StreamOffset.from(utf8(stream), afterId));
        List<StreamEntry> out = new ArrayList<>(messages.size());
        for (StreamMessage<byte[], byte[]> m : messages) {
            out.add(toEntry(m));
        }
        return out;
    }

    @Override
    public String xadd(String stream, Map<String, byte[]> fields) {
        Map<byte[], byte[]> body = new LinkedHashMap<>();
        fields.forEach((k, v) -> body.put(utf8(k), v));
        return commands.xadd(utf8(stream), body);
    }

    @Override
    public String lastEntryId(String stream) {
        List<StreamMessage<byte[], byte[]>> last = commands.xrevrange(utf8(stream), Range.create("-", "+"), Limit.from(1));
        return last.isEmpty() ? "0-0" : last.get(0).getId();
    }

    @Override
    public Map<Bytes, byte[]> hgetall(String key) {
        Map<Bytes, byte[]> out = new HashMap<>();
        commands.hgetall(utf8(key)).forEach((k, v) -> out.put(Bytes.wrap(k), v));
        return out;
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }

    private static StreamEntry toEntry(StreamMessage<byte[], byte[]> m) {
        Map<String, byte[]> fields = new HashMap<>();
        m.getBody().forEach((k, v) -> fields.put(new String(k, StandardCharsets.UTF_8), v));
        return new StreamEntry(m.getId(), fields);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

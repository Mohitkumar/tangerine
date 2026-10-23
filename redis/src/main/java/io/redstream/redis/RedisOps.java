package io.redstream.redis;

import java.util.List;
import java.util.Map;

/**
 * The small set of Redis operations the engine uses. Everything that must be atomic is a Lua
 * script; the rest is stream reads and appends. Keeping the surface this small makes the
 * client swappable (Lettuce today) and makes the engine easy to fake in tests.
 *
 * <p>Implementations must be safe to call from many (virtual) threads at once.
 */
public interface RedisOps extends AutoCloseable {

    /** Runs a script (EVALSHA, falling back to EVAL on NOSCRIPT) that returns an integer. */
    long evalLong(LuaScript script, List<String> keys, List<byte[]> args);

    /** Runs a script that returns an array of bulk strings. */
    List<byte[]> evalList(LuaScript script, List<String> keys, List<byte[]> args);

    /** XREAD COUNT n STREAMS key afterId, without BLOCK. Returns entries strictly after {@code afterId}. */
    List<StreamEntry> xread(String stream, String afterId, int count);

    /** XADD key * field value ...; returns the new entry ID. */
    String xadd(String stream, Map<String, byte[]> fields);

    /** ID of the newest entry, or {@code "0-0"} if the stream is empty or missing. */
    String lastEntryId(String stream);

    Map<Bytes, byte[]> hgetall(String key);

    @Override
    void close();
}

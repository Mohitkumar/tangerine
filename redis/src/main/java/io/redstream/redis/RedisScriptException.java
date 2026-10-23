package io.redstream.redis;

/** A Lua script returned an error reply. */
public class RedisScriptException extends RuntimeException {

    public RedisScriptException(String message, Throwable cause) {
        super(message, cause);
    }
}

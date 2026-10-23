package io.redstream.redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** A Lua script loaded from the classpath, with its SHA1 precomputed for EVALSHA. */
public record LuaScript(String name, String source, String sha1) {

    public static LuaScript load(Class<?> anchor, String resource) {
        try (InputStream in = anchor.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("script not found on classpath: " + resource);
            }
            String source = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return new LuaScript(resource, source, sha1(source));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sha1(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-1").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}

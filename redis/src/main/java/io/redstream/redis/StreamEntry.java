package io.redstream.redis;

import java.util.Map;

/** One Redis stream entry. Field names are UTF-8, values are raw bytes. */
public record StreamEntry(String id, Map<String, byte[]> fields) {

    public byte[] field(String name) {
        return fields.get(name);
    }

    /** Millisecond part of the entry ID, i.e. the time Redis appended it. */
    public long idMillis() {
        return Long.parseLong(id.substring(0, id.indexOf('-')));
    }
}

package io.redstream.runtime.state;

import io.redstream.redis.Bytes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Field name of a state value in the key group's {@code :state} hash:
 * {@code [operator id: u16][name length: u16][name][serialized key]}. The operator id keeps two
 * operators' state apart even if they use the same state name.
 */
public final class StateKeys {

    private StateKeys() {}

    public static Bytes encode(int operatorId, String stateName, byte[] key) {
        byte[] name = stateName.getBytes(StandardCharsets.UTF_8);
        if (operatorId > 0xffff || name.length > 0xffff) {
            throw new IllegalArgumentException("operator id or state name too large");
        }
        ByteBuffer buf = ByteBuffer.allocate(4 + name.length + key.length);
        buf.putShort((short) operatorId).putShort((short) name.length).put(name).put(key);
        return Bytes.wrap(buf.array());
    }
}

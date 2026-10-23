package io.redstream.redis;

/**
 * A write was rejected because the partition's epoch moved on: another worker now owns it.
 * The task that sees this must stop immediately and must not retry.
 */
public final class FencedException extends RuntimeException {

    public FencedException(String partition, String detail) {
        super("fenced on " + partition + ": " + detail);
    }
}

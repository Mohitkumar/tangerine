package io.redstream.api.time;

@FunctionalInterface
public interface TimestampAssigner<T> {

    /**
     * @param recordTimestamp the timestamp Redis assigned to the stream entry (millis from its ID)
     */
    long extractTimestamp(T element, long recordTimestamp);
}

package io.redstream.api.time;

/** Event-time timers for the current key. Timers are persisted with the partition's state. */
public interface TimerService {

    long currentWatermark();

    void registerEventTimeTimer(long timestamp);

    void deleteEventTimeTimer(long timestamp);
}

package io.redstream.api.window;

import java.util.Collection;

@FunctionalInterface
public interface WindowAssigner<T> {
    Collection<TimeWindow> assignWindows(T element, long timestamp);
}

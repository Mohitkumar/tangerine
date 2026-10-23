package io.redstream.runtime.operator;

import io.redstream.redis.KeySpace;
import io.redstream.redis.OutRecord;
import io.redstream.runtime.state.KeyedStateStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Per-partition context shared by the operators of one task, plus the batch being built. */
public final class TaskContext {

    private final String jobName;
    private final KeySpace keySpace;
    private final int maxParallelism;
    private final KeyedStateStore state;
    private final List<OutRecord> outputs = new ArrayList<>();
    private final Map<String, Long> seenUpdates = new HashMap<>();
    private long watermark = Long.MIN_VALUE;

    public TaskContext(String jobName, KeySpace keySpace, int maxParallelism, KeyedStateStore state) {
        this.jobName = jobName;
        this.keySpace = keySpace;
        this.maxParallelism = maxParallelism;
        this.state = state;
    }

    public String jobName() {
        return jobName;
    }

    public KeySpace keySpace() {
        return keySpace;
    }

    public int maxParallelism() {
        return maxParallelism;
    }

    /** Keyed state of this key group; {@code null} in a source stage. */
    public KeyedStateStore state() {
        return state;
    }

    public long watermark() {
        return watermark;
    }

    public void setWatermark(long watermark) {
        this.watermark = watermark;
    }

    public void emit(OutRecord record) {
        outputs.add(record);
    }

    public void markSeen(String producer, long seq) {
        seenUpdates.put(producer, seq);
    }

    public List<OutRecord> outputs() {
        return outputs;
    }

    public Map<String, Long> seenUpdates() {
        return seenUpdates;
    }

    public void resetBatch() {
        outputs.clear();
        seenUpdates.clear();
    }
}

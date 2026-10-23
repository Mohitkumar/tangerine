package io.redstream.redis;

import java.util.regex.Pattern;

/** Names every Redis key a job uses. Nothing else in the engine builds key strings. */
public final class KeySpace {

    private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");

    private final String job;

    public KeySpace(String job) {
        if (!NAME.matcher(job).matches()) {
            throw new IllegalArgumentException("invalid job name: " + job);
        }
        this.job = job;
    }

    public String job() {
        return job;
    }

    public PartitionKeys partition(int stage, int partition) {
        return new PartitionKeys(job, stage, partition);
    }

    /** Live workers: ZSET member = worker id, score = last heartbeat (Redis server time, ms). */
    public String workers() {
        return "{" + job + "}:workers";
    }
}

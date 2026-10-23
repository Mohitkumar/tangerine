package io.redstream.redis;

/**
 * Keys for one partition (a source partition or a key group). They share one hash tag, so in
 * Redis Cluster they live in the same slot and a single Lua script can update them together.
 *
 * <pre>
 *   {job:s1:p42}:meta    HASH   epoch (owner fencing)
 *   {job:s1:p42}:in      STREAM shuffled input (keyed stages only)
 *   {job:s1:p42}:lease   STRING owning worker id, with TTL
 * </pre>
 *
 * Plan step 5 moves keyed input to one stream per restart attempt, {@code :in:a<attempt>}.
 */
public record PartitionKeys(String job, int stage, int partition) {

    public String tag() {
        return "{" + job + ":s" + stage + ":p" + partition + "}";
    }

    public String meta() {
        return tag() + ":meta";
    }

    public String in() {
        return tag() + ":in";
    }

    public String lease() {
        return tag() + ":lease";
    }

    /** Stable id stamped on everything this partition emits; keyed tasks use it to align barriers. */
    public String producerId() {
        return "s" + stage + "p" + partition;
    }
}

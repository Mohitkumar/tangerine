package io.redstream.redis;

/**
 * Keys for one partition (a source partition or a key group). They share one hash tag, so in
 * Redis Cluster they live in the same slot and a single Lua script can update all of them
 * atomically. This colocation is what the whole commit protocol rests on.
 *
 * <pre>
 *   {job:s1:p42}:meta    HASH   epoch, in_offset, out_seq, fwd_offset, wm, seen:&lt;producer&gt;
 *   {job:s1:p42}:in      STREAM shuffled input (keyed stages only)
 *   {job:s1:p42}:state   HASH   keyed state
 *   {job:s1:p42}:out     STREAM outbox, drained by the forwarder
 *   {job:s1:p42}:timers  ZSET   event-time timers (milestone 4)
 *   {job:s1:p42}:lease   STRING owning worker id, with TTL
 * </pre>
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

    public String state() {
        return tag() + ":state";
    }

    public String out() {
        return tag() + ":out";
    }

    public String timers() {
        return tag() + ":timers";
    }

    public String lease() {
        return tag() + ":lease";
    }

    /** Stable id stamped on everything this partition emits; used for downstream dedup. */
    public String producerId() {
        return "s" + stage + "p" + partition;
    }
}

package io.redstream.runtime.fault;

import io.redstream.redis.PartitionKeys;
import java.util.SplittableRandom;

/** Crashes at each crash point with a fixed probability; seeded so failing runs can be replayed. */
public final class RandomFaults implements FaultHooks {

    private final SplittableRandom random;
    private final double probability;

    public RandomFaults(long seed, double probability) {
        this.random = new SplittableRandom(seed);
        this.probability = probability;
    }

    @Override
    public synchronized void at(CrashPoint point, PartitionKeys keys) {
        if (random.nextDouble() < probability) {
            throw new InjectedCrash(point, keys.tag());
        }
    }
}

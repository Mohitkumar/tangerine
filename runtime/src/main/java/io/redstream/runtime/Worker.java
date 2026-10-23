package io.redstream.runtime;

import io.redstream.api.JobDefinition;
import io.redstream.coord.Assignment;
import io.redstream.coord.LeaseManager;
import io.redstream.coord.Membership;
import io.redstream.coord.PartitionId;
import io.redstream.plan.JobGraph;
import io.redstream.plan.LogicalPlanner;
import io.redstream.redis.KeySpace;
import io.redstream.redis.LettuceRedisOps;
import io.redstream.redis.PartitionKeys;
import io.redstream.redis.RedisOps;
import io.redstream.runtime.fault.FaultHooks;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One engine process. Every worker runs the same job definition (library-style, like Kafka
 * Streams), plans it identically, and coordinates only through Redis: it heartbeats, works out
 * which partitions it should own, and acquires or releases their leases. Each owned partition
 * runs as a {@link PartitionTask} on a virtual thread.
 */
public final class Worker implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    private final JobGraph graph;
    private final KeySpace keySpace;
    private final RedisOps ops;
    private final WorkerConfig config;
    private final FaultHooks hooks;
    private final LeaseManager leases;
    private final Membership membership;
    private final List<PartitionId> partitions;
    private final Map<PartitionId, Running> running = new ConcurrentHashMap<>();
    private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService control = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("redstream-control").daemon().factory());

    private record Running(PartitionTask task, Future<?> future, long epoch) {}

    private Worker(JobDefinition job, WorkerConfig config, RedisOps ops, FaultHooks hooks) {
        this.graph = LogicalPlanner.plan(job);
        this.keySpace = new KeySpace(job.name());
        this.ops = ops;
        this.config = config;
        this.hooks = hooks;
        this.leases = new LeaseManager(ops, config.workerId(), config.leaseTtl());
        this.membership = new Membership(ops, keySpace, config.workerId(), config.leaseTtl());
        this.partitions = Assignment.allPartitions(graph.stages().stream()
                .mapToInt(s -> s.partitionCount(graph.maxParallelism())).toArray());
    }

    public static Worker start(JobDefinition job, WorkerConfig config) {
        RedisOps ops = config.cluster() ? LettuceRedisOps.cluster(config.redisUri()) : LettuceRedisOps.standalone(config.redisUri());
        return start(job, config, ops, FaultHooks.NONE);
    }

    /** For tests: supply the Redis client and fault hooks. The worker closes {@code ops} on close. */
    public static Worker start(JobDefinition job, WorkerConfig config, RedisOps ops, FaultHooks hooks) {
        Worker w = new Worker(job, config, ops, hooks);
        LOG.log(System.Logger.Level.INFO, "worker {0} starting job {1}\n{2}", config.workerId(), job.name(), w.graph.describe());
        long period = Math.max(100, config.leaseTtl().toMillis() / 3);
        w.control.scheduleWithFixedDelay(w::reconcile, 0, period, TimeUnit.MILLISECONDS);
        return w;
    }

    public JobGraph graph() {
        return graph;
    }

    /** Partitions with a live task on this worker. */
    public int ownedPartitions() {
        return (int) running.values().stream().filter(r -> !r.future.isDone()).count();
    }

    /** One control-loop pass: heartbeat, then converge owned partitions to the assignment. */
    void reconcile() {
        try {
            List<String> live = membership.heartbeat();
            for (int i = 0; i < partitions.size(); i++) {
                PartitionId pid = partitions.get(i);
                PartitionKeys keys = keySpace.partition(pid.stage(), pid.partition());
                Running r = running.get(pid);
                if (r != null && r.future.isDone()) {
                    running.remove(pid); // crashed or fenced; restart below if still ours
                    r = null;
                }
                boolean preferred = Assignment.preferredOwner(i, live).equals(config.workerId());
                if (preferred) {
                    OptionalLong epoch = leases.acquireOrRenew(keys);
                    if (epoch.isEmpty()) {
                        if (r != null) {
                            stop(pid, r); // someone else holds it: we were fenced out
                        }
                    } else if (r == null || r.epoch != epoch.getAsLong()) {
                        if (r != null) {
                            stop(pid, r);
                        }
                        startTask(pid, epoch.getAsLong());
                    }
                } else if (r != null) {
                    stop(pid, r);
                    leases.release(keys); // hand over promptly instead of waiting for TTL expiry
                }
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "reconcile failed; retrying next period", e);
        }
    }

    private void startTask(PartitionId pid, long epoch) {
        var task = new PartitionTask(graph, graph.stage(pid.stage()), pid.partition(), epoch, ops, keySpace, config, hooks);
        running.put(pid, new Running(task, tasks.submit(task), epoch));
    }

    private void stop(PartitionId pid, Running r) {
        r.task.stop();
        try {
            r.future.get(config.leaseTtl().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            r.future.cancel(true);
        } catch (Exception ignored) {
            // the task already logged its own failure
        }
        running.remove(pid);
    }

    @Override
    public void close() {
        control.shutdownNow();
        running.forEach((pid, r) -> {
            stop(pid, r);
            leases.release(keySpace.partition(pid.stage(), pid.partition()));
        });
        tasks.shutdown();
        ops.close();
    }
}

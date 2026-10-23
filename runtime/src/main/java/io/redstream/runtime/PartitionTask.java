package io.redstream.runtime;

import io.redstream.api.connector.RedisStreamSource;
import io.redstream.api.time.WatermarkStrategy;
import io.redstream.plan.JobGraph;
import io.redstream.plan.Stage;
import io.redstream.plan.StageInput;
import io.redstream.redis.CommitBatch;
import io.redstream.redis.CommitProtocol;
import io.redstream.redis.FencedException;
import io.redstream.redis.KeySpace;
import io.redstream.redis.MetaFields;
import io.redstream.redis.OutboxForwarder;
import io.redstream.redis.PartitionKeys;
import io.redstream.redis.PartitionSnapshot;
import io.redstream.redis.RedisOps;
import io.redstream.redis.StreamEntry;
import io.redstream.runtime.fault.CrashPoint;
import io.redstream.runtime.fault.FaultHooks;
import io.redstream.runtime.operator.OperatorChain;
import io.redstream.runtime.operator.TaskContext;
import io.redstream.runtime.state.KeyedStateStore;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Processes one partition (a source partition or a key group) for as long as this worker holds
 * its lease. The loop:
 *
 * <ol>
 *   <li>forward any committed-but-unforwarded outbox entries;</li>
 *   <li>read up to {@code batchSize} input entries after the committed offset;</li>
 *   <li>drop entries already seen from their producer (shuffle dedup);</li>
 *   <li>run them through the operator chain, buffering state changes and outputs;</li>
 *   <li>commit state, outputs, offset and seen-sequences in one Lua call, fenced by epoch.</li>
 * </ol>
 *
 * Any failure ends the task; the worker starts a fresh one that restores from Redis. There is
 * no partial rollback, because nothing reaches Redis except through step 5.
 */
public final class PartitionTask implements Runnable {

    private static final System.Logger LOG = System.getLogger(PartitionTask.class.getName());

    private final JobGraph graph;
    private final Stage stage;
    private final PartitionKeys keys;
    private final long epoch;
    private final RedisOps ops;
    private final KeySpace keySpace;
    private final WorkerConfig config;
    private final FaultHooks hooks;
    private final CommitProtocol protocol;
    private final OutboxForwarder forwarder;
    private volatile boolean running = true;

    public PartitionTask(JobGraph graph, Stage stage, int partition, long epoch, RedisOps ops, KeySpace keySpace,
                         WorkerConfig config, FaultHooks hooks) {
        this.graph = graph;
        this.stage = stage;
        this.keys = keySpace.partition(stage.id(), partition);
        this.epoch = epoch;
        this.ops = ops;
        this.keySpace = keySpace;
        this.config = config;
        this.hooks = hooks;
        this.protocol = new CommitProtocol(ops);
        this.forwarder = new OutboxForwarder(ops, protocol);
    }

    /** Asks the loop to exit after the current batch. */
    public void stop() {
        running = false;
    }

    public PartitionKeys keys() {
        return keys;
    }

    @Override
    public void run() {
        try {
            runLoop();
        } catch (FencedException e) {
            LOG.log(System.Logger.Level.INFO, "{0} lost ownership: {1}", keys.tag(), e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, keys.tag() + " failed; will restart from last commit", e);
        }
    }

    private void runLoop() throws Exception {
        String inputStream = inputStream();
        PartitionSnapshot snap = protocol.restore(keys, defaultInputOffset(inputStream));
        if (snap.epoch() != epoch) {
            throw new FencedException(keys.tag(), "epoch is " + snap.epoch() + " at restore, lease gave " + epoch);
        }

        KeyedStateStore state = stage.isKeyed() ? new KeyedStateStore(snap.state()) : null;
        TaskContext ctx = new TaskContext(graph.jobName(), keySpace, graph.maxParallelism(), state);
        ctx.setWatermark(snap.watermark());
        OperatorChain chain = OperatorChain.build(stage);
        chain.open(ctx);

        WatermarkStrategy<Object> watermarks = watermarkStrategy();
        Map<String, Long> seen = new HashMap<>(snap.seenSeqs());
        String inOffset = snap.inOffset();
        String fwdOffset = snap.fwdOffset();
        long maxTimestamp = Long.MIN_VALUE;

        LOG.log(System.Logger.Level.INFO, "{0} started at epoch {1}, offset {2}", keys.tag(), epoch, inOffset);
        while (running) {
            fwdOffset = forwardPending(fwdOffset);

            List<StreamEntry> entries = ops.xread(inputStream, inOffset, config.batchSize());
            if (entries.isEmpty()) {
                Thread.sleep(config.pollInterval());
                continue;
            }

            ctx.resetBatch();
            String last = inOffset;
            for (StreamEntry e : entries) {
                last = e.id();
                long ts;
                Object value;
                if (stage.isKeyed()) {
                    String producer = utf8(e.field(MetaFields.F_PRODUCER));
                    long seq = Long.parseLong(utf8(e.field(MetaFields.F_SEQ)));
                    if (seq <= seen.getOrDefault(producer, 0L)) {
                        continue; // duplicate from a re-forwarded outbox
                    }
                    seen.put(producer, seq);
                    ctx.markSeen(producer, seq);
                    value = chain.deserialize(e.field(MetaFields.F_VALUE));
                    ts = Long.parseLong(utf8(e.field(MetaFields.F_TS)));
                } else {
                    value = chain.deserialize(e.field(sourceSpec().payloadField()));
                    ts = watermarks.timestampAssigner().extractTimestamp(value, e.idMillis());
                    maxTimestamp = Math.max(maxTimestamp, ts);
                    if (watermarks.enabled()) {
                        ctx.setWatermark(Math.max(ctx.watermark(), maxTimestamp - watermarks.maxOutOfOrderness().toMillis()));
                    }
                }
                chain.process(value, ts);
            }
            hooks.at(CrashPoint.AFTER_READ, keys);

            CommitBatch batch = new CommitBatch(last, ctx.watermark(),
                    state == null ? Map.of() : state.pendingPuts(),
                    state == null ? Set.of() : state.pendingDeletes(),
                    ctx.outputs(), ctx.seenUpdates());
            hooks.at(CrashPoint.BEFORE_COMMIT, keys);
            protocol.commit(keys, epoch, batch);
            if (state != null) {
                state.onCommitted();
            }
            inOffset = last;
            hooks.at(CrashPoint.AFTER_COMMIT, keys);
        }
        forwardPending(fwdOffset);
    }

    private String forwardPending(String fromId) {
        String offset = fromId;
        while (true) {
            String next = forwarder.forwardOnce(keys, epoch, offset, config.batchSize(),
                    () -> hooks.at(CrashPoint.BEFORE_FORWARD_ACK, keys));
            if (next.equals(offset)) {
                return offset;
            }
            offset = next;
        }
    }

    private String inputStream() {
        return stage.isKeyed() ? keys.in() : sourceSpec().streams().get(keys.partition());
    }

    private String defaultInputOffset(String inputStream) {
        if (!stage.isKeyed() && sourceSpec().startPosition() == RedisStreamSource.StartPosition.LATEST) {
            return ops.lastEntryId(inputStream);
        }
        return "0-0";
    }

    private RedisStreamSource<?> sourceSpec() {
        return ((StageInput.FromSource) stage.input()).source().source();
    }

    @SuppressWarnings("unchecked")
    private WatermarkStrategy<Object> watermarkStrategy() {
        return stage.input() instanceof StageInput.FromSource s
                ? (WatermarkStrategy<Object>) s.source().watermarks()
                : WatermarkStrategy.noWatermarks();
    }

    private static String utf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }
}

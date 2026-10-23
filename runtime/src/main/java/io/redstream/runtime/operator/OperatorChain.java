package io.redstream.runtime.operator;

import io.redstream.api.serde.Serde;
import io.redstream.api.transformation.OneInputTransformation;
import io.redstream.api.transformation.OperatorSpec;
import io.redstream.api.transformation.PartitionTransformation;
import io.redstream.api.transformation.SinkTransformation;
import io.redstream.plan.ShuffleOutput;
import io.redstream.plan.Stage;
import io.redstream.plan.StageInput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Instantiates a stage's operators and wires them together. Built back to front: leaves
 * (sinks, shuffles) first, then operators in descending id order, so each operator's
 * downstream already exists when it is created.
 */
public final class OperatorChain {

    private final Operator<Object> head;
    private final Serde<Object> inputSerde;
    private final List<Operator<?>> all;

    private OperatorChain(Operator<Object> head, Serde<Object> inputSerde, List<Operator<?>> all) {
        this.head = head;
        this.inputSerde = inputSerde;
        this.all = all;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static OperatorChain build(Stage stage) {
        Map<Integer, List<Operator<?>>> consumersOf = new HashMap<>();
        List<Operator<?>> all = new ArrayList<>();

        for (SinkTransformation<?> s : stage.sinks()) {
            add(consumersOf, all, s.input().id(), new SinkOperator<>(s.sink()));
        }
        for (ShuffleOutput so : stage.shuffleOutputs()) {
            PartitionTransformation p = so.partition();
            add(consumersOf, all, p.input().id(), new ShuffleOperator(so.targetStageId(), p.keySelector(), p.keySerde(), p.valueSerde()));
        }

        PartitionTransformation<?, ?> keyedBy = stage.input() instanceof StageInput.FromShuffle fs ? fs.partition() : null;
        List<OneInputTransformation<?, ?>> ops = stage.operators();
        for (int i = ops.size() - 1; i >= 0; i--) {
            OneInputTransformation t = ops.get(i);
            Operator next = downstream(consumersOf, t.id());
            Operator op = switch (t.operator()) {
                case OperatorSpec.MapOp m -> new MapOperator(m.fn(), next);
                case OperatorSpec.FlatMapOp f -> new FlatMapOperator(f.fn(), next);
                case OperatorSpec.FilterOp f -> new FilterOperator(f.fn(), next);
                case OperatorSpec.KeyedProcessOp k -> new KeyedProcessOperator(t.id(), k.factory(), keyedBy.keySelector(), keyedBy.keySerde(), next);
                case OperatorSpec.WindowReduceOp w -> new WindowReduceOperator(w, next);
            };
            add(consumersOf, all, t.input().id(), op);
        }

        return switch (stage.input()) {
            case StageInput.FromSource s -> new OperatorChain(downstream(consumersOf, s.source().id()), (Serde) s.source().source().serde(), all);
            case StageInput.FromShuffle s -> new OperatorChain(downstream(consumersOf, s.partition().id()), (Serde) s.partition().valueSerde(), all);
        };
    }

    public void open(TaskContext ctx) throws Exception {
        for (Operator<?> op : all) {
            op.open(ctx);
        }
    }

    public Object deserialize(byte[] payload) {
        return inputSerde.deserialize(payload);
    }

    public void process(Object value, long timestamp) throws Exception {
        try {
            head.process(value, timestamp);
        } catch (Unchecked.Wrapped w) {
            throw (Exception) w.getCause();
        }
    }

    private static void add(Map<Integer, List<Operator<?>>> consumersOf, List<Operator<?>> all, int inputId, Operator<?> op) {
        consumersOf.computeIfAbsent(inputId, k -> new ArrayList<>()).add(op);
        all.add(op);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator downstream(Map<Integer, List<Operator<?>>> consumersOf, int id) {
        List<Operator<?>> c = consumersOf.getOrDefault(id, List.of());
        if (c.isEmpty()) {
            return (v, ts) -> {};
        }
        return c.size() == 1 ? c.get(0) : new FanOut(c);
    }
}

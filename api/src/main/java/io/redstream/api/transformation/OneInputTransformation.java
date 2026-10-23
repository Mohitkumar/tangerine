package io.redstream.api.transformation;

import io.redstream.api.serde.Serde;
import java.util.List;

/** A record-at-a-time operator; chained with its input unless the input is a partition edge. */
public record OneInputTransformation<I, O>(int id, String name, Transformation<I> input, OperatorSpec<I, O> operator, Serde<O> outputSerde)
        implements Transformation<O> {

    @Override
    public List<Transformation<?>> inputs() {
        return List.of(input);
    }

    public boolean isKeyed() {
        return operator instanceof OperatorSpec.KeyedProcessOp<?, ?, ?> || operator instanceof OperatorSpec.WindowReduceOp<?>;
    }
}

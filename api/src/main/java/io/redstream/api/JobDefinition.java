package io.redstream.api;

import io.redstream.api.transformation.Transformation;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The complete logical description of a job: what {@link StreamEnvironment#build} returns and
 * what every worker plans from. Workers rebuild it from the same {@code main()}, so it never
 * has to be serialized.
 *
 * @param maxParallelism number of key groups per keyed stage; fixed for the job's lifetime
 */
public record JobDefinition(String name, int maxParallelism, List<Transformation<?>> transformations) {

    /** Job names become part of Redis hash tags, so braces and colons are not allowed. */
    private static final Pattern NAME = Pattern.compile("[a-z0-9][a-z0-9-]{0,62}");

    public JobDefinition {
        Objects.requireNonNull(name, "name");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("job name must match " + NAME.pattern() + ": " + name);
        }
        transformations = List.copyOf(transformations);
        if (transformations.isEmpty()) {
            throw new IllegalArgumentException("job has no transformations");
        }
    }
}

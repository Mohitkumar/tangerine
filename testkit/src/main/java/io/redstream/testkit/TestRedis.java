package io.redstream.testkit;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A Redis for integration tests. Uses {@code REDSTREAM_REDIS_URI} if set (e.g. a local
 * redis-server), otherwise starts a container. Tests must use unique job names, since the
 * database is shared and not flushed.
 */
public final class TestRedis implements AutoCloseable {

    private static final String IMAGE = "redis:7.4-alpine";

    private final GenericContainer<?> container;
    private final String uri;

    private TestRedis(GenericContainer<?> container, String uri) {
        this.container = container;
        this.uri = uri;
    }

    public static TestRedis start() {
        String external = System.getenv("REDSTREAM_REDIS_URI");
        if (external != null && !external.isBlank()) {
            return new TestRedis(null, external);
        }
        GenericContainer<?> c = new GenericContainer<>(DockerImageName.parse(IMAGE)).withExposedPorts(6379);
        c.start();
        return new TestRedis(c, "redis://" + c.getHost() + ":" + c.getMappedPort(6379));
    }

    public String uri() {
        return uri;
    }

    @Override
    public void close() {
        if (container != null) {
            container.stop();
        }
    }
}

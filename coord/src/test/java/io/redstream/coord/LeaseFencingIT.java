package io.redstream.coord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.redstream.redis.Bytes;
import io.redstream.redis.CommitBatch;
import io.redstream.redis.CommitProtocol;
import io.redstream.redis.FencedException;
import io.redstream.redis.KeySpace;
import io.redstream.redis.LettuceRedisOps;
import io.redstream.redis.PartitionKeys;
import io.redstream.testkit.TestRedis;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class LeaseFencingIT {

    @Test
    void workerThatStallsPastItsLeaseCannotCommit() throws Exception {
        try (var redis = TestRedis.start(); var ops = LettuceRedisOps.standalone(redis.uri())) {
            PartitionKeys keys = new KeySpace("fence-" + UUID.randomUUID().toString().substring(0, 8)).partition(1, 7);
            var w1 = new LeaseManager(ops, "w1", Duration.ofMillis(300));
            var w2 = new LeaseManager(ops, "w2", Duration.ofMillis(300));

            long e1 = w1.acquireOrRenew(keys).orElseThrow();
            assertTrue(w2.acquireOrRenew(keys).isEmpty(), "lease is exclusive while held");
            assertEquals(e1, w1.acquireOrRenew(keys).orElseThrow(), "renewal keeps the epoch");

            Thread.sleep(400); // w1 stalls past its TTL
            long e2 = w2.acquireOrRenew(keys).orElseThrow();
            assertEquals(e1 + 1, e2, "takeover bumps the epoch");

            var protocol = new CommitProtocol(ops);
            var zombie = new CommitBatch("5-0", 0, Map.of(Bytes.utf8("k"), "zombie".getBytes(StandardCharsets.UTF_8)),
                    Set.of(), List.of(), Map.of());
            assertThrows(FencedException.class, () -> protocol.commit(keys, e1, zombie));
            assertThrows(FencedException.class, () -> protocol.ackForwarded(keys, e1, "1-0"));
            assertTrue(protocol.restore(keys, "0-0").state().isEmpty(), "fenced commit wrote nothing");
            assertTrue(w1.acquireOrRenew(keys).isEmpty());
        }
    }
}

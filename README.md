# Redstream

A Flink-style stream processing engine built on Redis, in Java 21.

Redstream gives you a DataStream API (`map`, `flatMap`, `filter`, `keyBy`, keyed process
functions with state, event-time windows) and runs it on a set of identical worker processes
that coordinate only through Redis. Redis Streams carry the data, Redis hashes hold the keyed
state, and Lua scripts commit each processed batch atomically, which gives exactly-once state
updates without Flink's global checkpoints.

```java
var env = StreamEnvironment.create().maxParallelism(128);

env.fromSource(RedisStreamSource.of(Serdes.string(), "lines:0", "lines:1"), WatermarkStrategy.noWatermarks(), "lines")
   .flatMap("split", (String line, Collector<String> out) -> {
       for (String w : line.toLowerCase().split("\\W+")) if (!w.isEmpty()) out.collect(w);
   }, Serdes.string())
   .keyBy(w -> w, Serdes.string())
   .process("count", CountWords::new, Count.SERDE)
   .sinkTo(RedisStreamSink.of("counts", Count.SERDE));

Worker.start(env.build("wordcount"), WorkerConfig.fromEnv());
```

Start the same program on more machines to scale out.

## How it works, in one paragraph

Each **partition** (a source stream, or one of the fixed key groups of a keyed stage) keeps
its input offset, keyed state and pending outputs under a single Redis hash tag, so they share
a hash slot. A worker processes a batch of records in memory, then commits the state changes,
the outputs and the new offset in **one Lua script call**: all of it lands or none of it does.
Records crossing a `keyBy` go through a per-partition outbox and carry a (producer, sequence)
pair, so re-delivery after a crash is detected and dropped downstream. Partitions are owned
through TTL leases, and every commit is **fenced by an epoch**, so a worker that stalls and
loses its lease can never overwrite the new owner's state.

Read [DESIGN.md](DESIGN.md) for the full design: the commit protocol and its invariants, the
shuffle, leases and fencing, recovery, and the failure model.

## Modules

| Module | What it contains |
|---|---|
| `api` | `StreamEnvironment`, `DataStream`, `KeyedStream`, functions, serdes, state, windows, transformations. No dependencies. |
| `plan` | `LogicalPlanner` (transformations → stages, operator chaining), key groups, Murmur3 |
| `redis` | Key layout, `RedisOps` client interface + Lettuce implementation, `CommitProtocol`, outbox forwarder, `commit.lua`, `forward_ack.lua` |
| `coord` | Leases with epochs, membership heartbeats, partition assignment (`lease.lua`, `release.lua`, `heartbeat.lua`) |
| `runtime` | `Worker`, `PartitionTask` loop, operators and chaining, keyed state store, fault-injection hooks |
| `testkit` | `TestRedis` (local Redis or Testcontainers), `OutputChecker` |
| `examples` | `WordCount` |

Dependencies: `plan → api`, `redis → api`, `coord → redis`, `runtime → plan, redis, coord`,
`examples → runtime`. Tests use `testkit`.

## Requirements

- JDK 21
- Redis 7+ or Valkey 7.2+ (standalone or cluster). Redis 6.2 is the minimum (`XTRIM MINID`).
- Docker, only if you run integration tests without your own Redis

## Build and test

```bash
./gradlew build                 # compile + unit tests (no Redis needed)
./gradlew integrationTest       # needs Redis: uses REDSTREAM_REDIS_URI, else starts a container
```

```bash
# integration tests against a local Redis instead of Docker
redis-server --port 6379 &
REDSTREAM_REDIS_URI=redis://localhost:6379 ./gradlew integrationTest
```

`ExactlyOnceIT` runs word count with random crashes injected at every crash point, kills a
worker mid-run, adds a new one, and checks the final counts against an in-memory reference.

## Run the example

```bash
redis-server &
./gradlew :examples:run                                   # worker 1
REDSTREAM_WORKER_ID=w2 ./gradlew :examples:run            # worker 2, in another terminal

redis-cli XADD wc:lines:0 '*' v "the quick brown fox"
redis-cli XADD wc:lines:1 '*' v "the lazy dog"
redis-cli XRANGE wc:counts - +
```

Worker settings come from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `REDSTREAM_REDIS_URI` | `redis://localhost:6379` | Redis to connect to |
| `REDSTREAM_CLUSTER` | `false` | Use the Redis Cluster client |
| `REDSTREAM_WORKER_ID` | random | Must be unique per running worker |
| `REDSTREAM_BATCH_SIZE` | `500` | Records per read and per atomic commit |

## Things to know when writing jobs

- **Serdes are explicit.** Every type-changing operator takes a `Serde`, because Java erases
  generic types at runtime. Key serdes must be deterministic: the key group is computed from
  the serialized key bytes.
- **Keyed functions take a supplier.** Use `.process("name", MyFunction::new, serde)`. A worker
  runs many key groups in one JVM, and each needs its own function instance because state
  handles live in fields. Passing a supplier that returns a shared instance fails at startup.
- **Stateless functions are shared.** Lambdas given to `map`, `flatMap`, `filter` and `keyBy` are
  used by all partitions on a worker concurrently. Keep them stateless.
- **Declare state in `open()`**, not in `processElement`.
- **`maxParallelism` is permanent.** It is the number of key groups and caps how many workers a
  keyed stage can use. Pick it generously (128 or more) up front.
- **Sinks are at-least-once into the stream, exactly-once with dedup.** Each sink entry carries
  `p` (producer) and `s` (sequence). Keep the highest `s` per `p` to drop duplicates.

## Status

Implemented: DataStream API, planner with operator chaining, source and keyed stages, keyed
`ValueState`, atomic per-batch commits, keyBy shuffle with dedup, leases with epoch fencing,
membership and assignment, crash-point fault injection.

Not yet: event-time timers and windowed reduce (the API exists; the runtime raises
`UnsupportedOperationException`), watermark propagation across the shuffle, backpressure,
lazy state loading, joins. See the milestones in [DESIGN.md](DESIGN.md#15-milestones).

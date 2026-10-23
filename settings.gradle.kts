rootProject.name = "redstream"

// Module layout (dependencies flow downward):
//   api      – user-facing DataStream API, functions, serdes, transformations
//   plan     – logical planning: transformations -> stages, key groups
//   redis    – Redis key layout, Lua scripts, commit protocol, stream I/O
//   coord    – leases, epochs, membership, partition assignment
//   runtime  – workers, partition tasks, operators, state overlay
//   testkit  – Redis test container, fault injection, output checking
//   examples – runnable jobs (word count, ...)
listOf("api", "plan", "redis", "coord", "runtime", "testkit", "examples").forEach { include(":$it") }

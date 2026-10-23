// Test support shared by other modules' tests. Depends only on api and the Redis client, so
// redis and coord tests can use it without a cycle. Fault-injection hooks live in runtime.
dependencies {
    api(project(":api"))
    api(libs.lettuce.core)
    api(libs.testcontainers)
}

dependencies {
    api(project(":api"))
    api(libs.lettuce.core)
    testImplementation(project(":testkit"))
}

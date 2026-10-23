plugins {
    application
}

dependencies {
    implementation(project(":runtime"))
}

application {
    mainClass.set("io.redstream.examples.WordCount")
}

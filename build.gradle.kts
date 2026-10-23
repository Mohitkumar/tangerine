plugins {
    `java-library`
}

allprojects {
    group = "io.redstream"
    version = "0.1.0-SNAPSHOT"
}

val catalog = extensions.getByType<VersionCatalogsExtension>().named("libs")

subprojects {
    apply(plugin = "java-library")

    repositories { mavenCentral() }

    extensions.configure<JavaPluginExtension> {
        toolchain { languageVersion.set(JavaLanguageVersion.of(25)) }
    }

    extensions.configure<BasePluginExtension> {
        archivesName.set("redstream-${project.name}")
    }

    dependencies {
        "testImplementation"(platform(catalog.findLibrary("junit-bom").get()))
        "testImplementation"(catalog.findLibrary("junit-jupiter").get())
        "testRuntimeOnly"(catalog.findLibrary("junit-launcher").get())
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing,-fallthrough", "-parameters"))
    }

    // Unit tests run by default. Tests tagged "integration" need a real Redis
    // (REDSTREAM_REDIS_URI, or Docker for Testcontainers): ./gradlew integrationTest
    tasks.named<Test>("test") {
        useJUnitPlatform { excludeTags("integration") }
    }
    tasks.register<Test>("integrationTest") {
        description = "Runs tests that need a live Redis."
        group = "verification"
        val sourceSets = project.extensions.getByType<SourceSetContainer>()
        testClassesDirs = sourceSets["test"].output.classesDirs
        classpath = sourceSets["test"].runtimeClasspath
        useJUnitPlatform { includeTags("integration") }
    }
}

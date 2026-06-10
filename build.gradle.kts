import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    application
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.radar.dependency.management)
    alias(libs.plugins.radar.publishing)
    alias(libs.plugins.radar.kotlin)
    alias(libs.plugins.radar.root.project)
    alias(libs.plugins.version.catalog.update)
    `jvm-test-suite`
}

description = "RADAR-base service for Kafka stream processing, monitoring, and statistics utilities."

radarRootProject {
    projectVersion.set(properties["projectVersion"] as String)
    gradleVersion.set(properties["gradleVersion"] as String)
}

radarPublishing {
    val githubRepoName = "RADAR-base/RADAR-Backend"
    githubUrl.set("https://github.com/$githubRepoName.git")
    developers {
        developer {
            id.set("pvannierop")
            name.set("Pim van Nierop")
            email.set("pim@thehyve.nl")
            organization.set("The Hyve")
        }
        developer {
            id.set("yatharthranjan")
            name.set("Yatharth Ranjan")
            email.set("yatharth.ranjan@kcl.ac.uk")
            organization.set("Institute of Psychiatry, Psychology & Neuroscience, King's College London")
        }
    }
}

radarKotlin {
    log4j2Version.set(libs.versions.log4j)
    sentryEnabled.set(true)
    openTelemetryAgentEnabled.set(false)
}

dependencies {
    implementation(libs.radar.commons)
    implementation(libs.radar.commons.server)
    implementation(libs.avro)
    implementation(libs.radar.commons.testing)
    implementation(libs.radar.schemas.commons)

    implementation(libs.kotlin.stdlib)

    implementation(libs.kafka.streams)
    implementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "jline", module = "jline")
        exclude(group = "io.netty", module = "netty")
    }

    implementation(libs.jsr305)
    implementation(libs.commons.cli)
    implementation(libs.jackson.annotations)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.jakarta.mail)
    implementation(libs.bundles.ksoup)

    // Testing
    testImplementation(libs.mockito.core)

    // Mock mail server
    testImplementation(libs.greenmail)

    testImplementation(kotlin("test"))
    testImplementation(libs.junit.jupiter.params)
}

// --- Vulnerability fixes start ---
configurations.all {
    resolutionStrategy.dependencySubstitution {
        // Substitute the old group/module with drop-in replacement
        substitute(module("org.lz4:lz4-java"))
            .using(module(libs.lz4.get().toString()))
            .because("Force safe version of LZ4 across all modules")
    }
}
// --- Vulnerability fixes end ---

kotlin {
    jvmToolchain(21)
}

testing {
    suites {
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter()
            targets {
                all {
                    testTask {
                        testLogging {
                            showStandardStreams = true
                            showExceptions = true
                            showCauses = true
                            showStackTraces = true
                            exceptionFormat = TestExceptionFormat.FULL
                            events("skipped", "failed")
                        }
                    }
                }
            }
        }
        register<JvmTestSuite>("integrationTest") {
            description = "Run integration tests (located in src/integrationTest/...)."
            dependencies {
                implementation(project())
            }
            targets {
                all {
                    testTask {
                        shouldRunAfter(test)
                        testLogging {
                            events("passed", "skipped", "failed")
                        }
                    }
                }
            }
        }
    }
}

// As part of check task, compile the integration test code
tasks.named("check") {
    dependsOn(tasks.named("integrationTest"))
}

configurations["integrationTestImplementation"].extendsFrom(configurations.testImplementation.get())

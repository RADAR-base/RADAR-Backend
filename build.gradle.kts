plugins {
    application
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.radar.dependency.management)
    alias(libs.plugins.radar.publishing)
    alias(libs.plugins.radar.kotlin)
    alias(libs.plugins.radar.root.project)
    alias(libs.plugins.version.catalog.update)
}

description = "RADAR-base service for Kafka stream processing, monitoring, and statistics utilities."

radarRootProject {
    projectVersion.set(properties["projectVersion"] as String)
    gradleVersion.set(properties["gradleVersion"] as String)
}

radarPublishing {
    val githubRepoName = "RADAR-base/radar-backend"
    githubUrl.set("https://github.com/$githubRepoName.git")
    developers {
        developer {
            id.set("pvannierop")
            name.set("Pim van Nierop")
            email.set("pim@thehyve.nl")
            organization.set("The Hyve")
        }
    }
}

radarKotlin {
    log4j2Version.set(libs.versions.log4j)
    sentryEnabled.set(true)
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
    implementation(libs.jakarta.mail)
    implementation(libs.bundles.ksoup)

    // Testing
    testImplementation(libs.mockito.core)

    // Mock mail server
    testImplementation(libs.greenmail)

    testImplementation(kotlin("test"))
    testImplementation(libs.junit.jupiter.params)
}

kotlin {
    jvmToolchain(21)
}

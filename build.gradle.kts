plugins {
    application
    alias(libs.plugins.radar.dependency.management)
    alias(libs.plugins.radar.publishing)
    alias(libs.plugins.radar.kotlin)
    alias(libs.plugins.radar.root.project)
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
    sentryEnabled.set(true)
}

application {
    mainClass.set("org.radarbase.RadarBackend")
    applicationDefaultJvmArgs = listOf("-Dlog4j.configuration=log4j.properties")
}

repositories {
    mavenCentral()
    maven { url = uri("https://packages.confluent.io/maven/") }
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

    runtimeOnly(libs.log4j)
    runtimeOnly(libs.slf4j.log4j)

    // Testing
    testImplementation(libs.junit)
    testImplementation(libs.mockito.core)
    testImplementation(libs.hamcrest.all)

    // Mock mail server
    testImplementation(libs.greenmail)

    // Using the bundle for Logging
    testImplementation(libs.bundles.logging.log4j)
}


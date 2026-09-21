package org.radarbase.stream.ruleengine.harness

import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * A single Testcontainers-managed Kafka broker (KRaft, single node), started once and shared
 * across every test class that uses [KafkaBrokerExtension]. The image tag matches this repo's
 * production Kafka client version (gradle/libs.versions.toml: confluent = "7.9.10") so the
 * harness exercises the same broker generation as the real deployment. Testcontainers' Ryuk
 * reaper stops the container when the JVM exits; there is no explicit stop() call.
 */
object KafkaBroker {
    private const val IMAGE = "confluentinc/cp-kafka:7.9.10"

    val container: ConfluentKafkaContainer by lazy {
        ConfluentKafkaContainer(DockerImageName.parse(IMAGE)).apply { start() }
    }

    val bootstrapServers: String
        get() = container.bootstrapServers
}

/** Register with `@ExtendWith(KafkaBrokerExtension::class)` to start the shared broker up front. */
class KafkaBrokerExtension : BeforeAllCallback {
    override fun beforeAll(context: ExtensionContext) {
        KafkaBroker.container
    }
}

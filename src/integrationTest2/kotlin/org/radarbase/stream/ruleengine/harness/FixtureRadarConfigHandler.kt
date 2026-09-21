package org.radarbase.stream.ruleengine.harness

import org.radarbase.config.KafkaProperty
import org.radarbase.config.RadarBackendConfig
import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.ServerConfig
import org.radarbase.config.StreamConfig
import org.radarbase.util.PersistentStateStore

/**
 * A [RadarConfigHandler] built in-memory from a Testcontainers broker address, standing in for
 * the `radar.yml`-backed [org.radarbase.config.RadarConfigHandlerImpl] used in production. Only
 * `broker` is real; `schema_registry` is a placeholder because the rule-engine stream's global
 * store uses explicit key/value Serdes (see [org.radarbase.stream.ruleengine.RuleEngineStream])
 * and never actually contacts it.
 */
class FixtureRadarConfigHandler(bootstrapServers: String) : RadarConfigHandler {
    override val radarProperties: RadarBackendConfig = run {
        // KafkaContainer.bootstrapServers may or may not carry a "PLAINTEXT://" scheme prefix
        // depending on the Testcontainers version; java.net.URI misparses a bare "host:port" as
        // an opaque URI (scheme="host", no host/port), so split on the last ':' instead.
        val hostPort = bootstrapServers.substringAfter("://")
        val host = hostPort.substringBeforeLast(':')
        val port = hostPort.substringAfterLast(':').toInt()
        RadarBackendConfig(
            broker = listOf(
                ServerConfig().apply {
                    this.host = host
                    this.port = port
                },
            ),
            schemaRegistry = listOf(
                ServerConfig().apply {
                    this.host = "unused-schema-registry"
                    this.port = 1
                    protocol = "http"
                },
            ),
            stream = StreamConfig(),
            buildVersion = "integrationTest2",
        )
    }

    override val kafkaProperties: KafkaProperty by lazy { KafkaProperty(radarProperties) }

    override fun load(pathFile: String?) {
        // Fixture config is already loaded in-memory; nothing to do.
    }

    override fun isLoaded(): Boolean = true

    override fun getPersistentStateStore(): PersistentStateStore? = null
}

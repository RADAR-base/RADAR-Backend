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
 * `broker` is real by default; `schema_registry` is a placeholder because the rule-engine
 * stream's global store uses explicit key/value Serdes (see
 * [org.radarbase.stream.ruleengine.RuleEngineStream]) and never actually contacts it. Pass
 * [schemaRegistryUrl] (e.g. `mock://<scope>`, backed by Confluent's in-memory
 * `MockSchemaRegistryClient`) for tests that exercise the `input_topic` side of the topology,
 * where the default `SpecificAvroSerde` key/value serdes need a real schema registry to decode
 * Avro records.
 */
class FixtureRadarConfigHandler(
    bootstrapServers: String,
    schemaRegistryUrl: String = "http://unused-schema-registry:1",
) : RadarConfigHandler {
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
                    this.protocol = schemaRegistryUrl.substringBefore("://")
                    this.host = schemaRegistryUrl.substringAfter("://")
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

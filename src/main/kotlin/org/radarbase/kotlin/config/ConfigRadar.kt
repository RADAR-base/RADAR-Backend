package org.radarbase.kotlin.config

import com.fasterxml.jackson.annotation.JsonProperty
import org.radarbase.config.BatteryMonitorConfig
import org.radarbase.config.DisconnectMonitorConfig
import org.radarbase.config.SourceStatisticsStreamConfig
import org.radarbase.config.StreamConfig
import org.radarbase.config.YamlConfigLoader
import java.util.Date

/**
 * POJO representing the yml file
 */
class ConfigRadar {
    var released: Date? = null
    var version: String? = null
    var zookeeper: List<org.radarbase.config.ServerConfig>? = null
    var broker: List<org.radarbase.config.ServerConfig>? = null

    @JsonProperty("schema_registry")
    var schemaRegistry: List<org.radarbase.config.ServerConfig>? = null

    @JsonProperty("rest_proxy")
    var restProxy: org.radarbase.config.ServerConfig? = null

    @JsonProperty("battery_monitor")
    var batteryMonitor: BatteryMonitorConfig? = null

    @JsonProperty("disconnect_monitor")
    var disconnectMonitor: DisconnectMonitorConfig? = null

    @JsonProperty("statistics_monitors")
    var statisticsMonitors: List<SourceStatisticsStreamConfig>? = null

    @JsonProperty("stream")
    var stream: StreamConfig? = null

    @JsonProperty("persistence_path")
    var persistencePath: String? = null

    var extras: Map<String, Any>? = null

    @JsonProperty("build_version")
    var buildVersion: String? = null

    fun getZookeeperPaths(): String = zookeeper?.let { org.radarbase.config.ServerConfig.getPaths(it) }
        ?: throw IllegalStateException("'zookeeper' is not configured")

    fun getBrokerPaths(): String = broker?.let { org.radarbase.config.ServerConfig.getPaths(it) }
        ?: throw IllegalStateException("Kafka 'broker' is not configured")

    fun getSchemaRegistryPaths(): String = schemaRegistry?.let { org.radarbase.config.ServerConfig.getPaths(it) }
        ?: throw IllegalStateException("'schema_registry' is not configured")

    fun getRestProxyPath(): String = restProxy?.path
        ?: throw IllegalStateException("'rest_proxy' is not configured")

    override fun toString(): String = YamlConfigLoader().prettyString(this)
}
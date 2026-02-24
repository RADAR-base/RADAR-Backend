/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

/**
 * POJO representing the yml file
 */
class ConfigRadar {
    var released: Date? = null
    var version: String? = null
    var zookeeper: List<ServerConfig>? = null
    var broker: List<ServerConfig>? = null

    @JsonProperty("schema_registry")
    var schemaRegistry: List<ServerConfig>? = null

    @JsonProperty("rest_proxy")
    var restProxy: ServerConfig? = null

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

    val zookeeperPaths: String
        get() = zookeeper?.let { ServerConfig.getPaths(it) } ?: throw IllegalStateException("'zookeeper' is not configured")

    val brokerPaths: String
        get() = broker?.let { ServerConfig.getPaths(it) } ?: throw IllegalStateException("Kafka 'broker' is not configured")

    val schemaRegistryPaths: String
        get() = schemaRegistry?.let { ServerConfig.getPaths(it) } ?: throw IllegalStateException("'schema_registry' is not configured")

    val restProxyPath: String
        get() {
            checkNotNull(restProxy) { "'rest_proxy' is not configured" }
            return restProxy!!.path
        }

    override fun toString(): String = YamlConfigLoader().prettyString(this)
}

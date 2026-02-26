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

package org.radarbase.monitor

import org.radarbase.config.MonitorConfig
import org.radarbase.config.NotifyConfig
import org.radarbase.config.RadarBackendOptions
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.util.EmailSenders
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.stream.Stream

class KafkaMonitorFactory(
    private val options: RadarBackendOptions,
    private val properties: RadarPropertyHandler,
) {
    @Throws(IOException::class)
    fun createMonitor(): KafkaMonitor {
        val args = options.subCommandArgs
        val commandType = if (args == null || args.isEmpty()) "all" else args[0]

        val monitor = when (commandType) {
            "battery" -> createBatteryLevelMonitor()
            "disconnect" -> createDisconnectMonitor()
            "all" -> CombinedKafkaMonitor(Stream.of(createDisconnectMonitor(), createBatteryLevelMonitor()))
            else -> throw IllegalArgumentException("Cannot create unknown monitor $commandType")
        }
        return monitor ?: throw IllegalArgumentException("Monitor $commandType is not configured.")
    }

    private fun createBatteryLevelMonitor(): KafkaMonitor? {
        val config = properties.radarProperties.batteryMonitor ?: run {
            logger.warn("Battery level monitor is not configured. Cannot start it.")
            return null
        }

        var minLevel = BatteryLevelMonitor.Status.CRITICAL
        val senders = getSenders(config)
        val topics = getTopics(config, "android_empatica_e4_battery_level")

        config.level?.let {
            val level = it.uppercase(Locale.US)
            try {
                minLevel = BatteryLevelMonitor.Status.valueOf(level)
            } catch (ex: IllegalArgumentException) {
                logger.warn(
                    "Minimum battery level $level is not recognized. Choose from ${
                        BatteryLevelMonitor.Status.values().contentToString()
                    } instead. Using CRITICAL.",
                )
            }
        }
        val logInterval = config.logInterval.toLong()

        return BatteryLevelMonitor(properties, topics, senders, minLevel, logInterval)
    }

    private fun createDisconnectMonitor(): KafkaMonitor? {
        val config = properties.radarProperties.disconnectMonitor ?: run {
            logger.warn("Disconnect monitor is not configured. Cannot start it.")
            return null
        }
        val senders = getSenders(config)
        val topics = getTopics(config, "android_empatica_e4_temperature")
        return DisconnectMonitor(properties, topics, "disconnect_monitor", senders)
    }

    private fun getSenders(config: MonitorConfig?): EmailSenders? {
        if (config?.notifyConfig == null) {
            return null
        }
        val javaConfig = MonitorConfig()
        javaConfig.notifyConfig = config.notifyConfig?.map { c -> NotifyConfig(c.projectId, c.emailAddress) }
        javaConfig.emailHost = config.emailHost
        javaConfig.emailPort = config.emailPort
        javaConfig.emailUser = config.emailUser
        javaConfig.logInterval = config.logInterval
        javaConfig.message = config.message
        javaConfig.topics = config.topics
        return EmailSenders.Companion.parseConfig(javaConfig)
    }

    private fun getTopics(config: MonitorConfig?, defaultTopic: String): Collection<String> {
        return config?.topics ?: listOf(defaultTopic)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaMonitorFactory::class.java)
    }
}

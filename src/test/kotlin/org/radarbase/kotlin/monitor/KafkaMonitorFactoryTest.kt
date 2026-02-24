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

package org.radarbase.kotlin.monitor

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.Assert.*
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.radarbase.config.RadarPropertyHandler as JavaRadarPropertyHandler
import org.radarbase.config.YamlConfigLoader
import org.radarbase.kotlin.config.*
import org.radarbase.kotlin.config.RadarPropertyHandler as KotlinRadarPropertyHandler
import org.radarbase.kotlin.config.RadarPropertyHandlerImpl as KotlinRadarPropertyHandlerImpl
import org.radarbase.util.EmailServerRule
import java.io.File
import java.io.IOException
import java.util.*
import java.util.stream.Stream

class KafkaMonitorFactoryTest {
    @Rule
    @JvmField
    val folder = TemporaryFolder()

    @Test
    fun createBatteryMonitor() {
        val args = arrayOf("monitor", "battery")
        val options = RadarBackendOptions.parse(args)
        val config = getBatteryMonitorConfig(emailServer.port, folder)
        val properties = getRadarPropertyHandler(config, folder)

        val monitor = KafkaMonitorFactory(options, properties).createMonitor()
        assertEquals(BatteryLevelMonitor::class.java, monitor.javaClass)
        val batteryMonitor = monitor as BatteryLevelMonitor
        batteryMonitor.evaluateRecords(ConsumerRecords(emptyMap()))
        assertTrue(File(config.persistencePath!!, "battery_monitors_" +
                BatteryLevelMonitor::class.java.name + "-1.yml").isFile)
    }

    @Test(expected = IOException::class)
    fun createBatteryMonitorWithoutEmailServer() {
        val args = arrayOf("monitor", "battery")
        val options = RadarBackendOptions.parse(args)
        val config = getBatteryMonitorConfig(emailServer.port + 1, folder)
        val properties = getRadarPropertyHandler(config, folder)

        KafkaMonitorFactory(options, properties).createMonitor()
    }

    @Test
    fun createDisconnectMonitor() {
        val args = arrayOf("monitor", "disconnect")
        val options = RadarBackendOptions.parse(args)
        val config = getDisconnectMonitorConfig(emailServer.port, folder)
        val properties = getRadarPropertyHandler(config, folder)

        val monitor = KafkaMonitorFactory(options, properties).createMonitor()
        assertEquals(DisconnectMonitor::class.java, monitor.javaClass)
        val disconnectMonitor = monitor as DisconnectMonitor
        disconnectMonitor.evaluateRecords(ConsumerRecords(emptyMap()))
        assertTrue(File(config.persistencePath!!, "disconnect_monitor_" +
                DisconnectMonitor::class.java.name + "-1.yml").isFile)
    }

    @Test
    fun createAllMonitor() {
        val args = arrayOf("monitor", "all")
        val options = RadarBackendOptions.parse(args)
        val config = createBasicConfig(folder)
        config.batteryMonitor = getBatteryMonitorConfig(emailServer.port)
        config.disconnectMonitor = getDisconnectMonitorConfig(emailServer.port)
        val properties = getRadarPropertyHandler(config, folder)

        val monitor = KafkaMonitorFactory(options, properties).createMonitor()
        assertEquals(CombinedKafkaMonitor::class.java, monitor.javaClass)
        val combinedMonitor = monitor as CombinedKafkaMonitor
        val monitors = combinedMonitor.getMonitors()
        assertEquals(2, monitors.size)
        assertTrue(monitors.any { it is BatteryLevelMonitor })
        assertTrue(monitors.any { it is DisconnectMonitor })
        assertNotEquals(monitors[0].javaClass, monitors[1].javaClass)
    }

    companion object {
        @ClassRule
        @JvmField
        val emailServer = EmailServerRule(25251)

        @Throws(IOException::class)
        fun getRadarPropertyHandler(config: ConfigRadar, folder: TemporaryFolder): KotlinRadarPropertyHandler {
            val tmpConfig = folder.newFile("radar.yml")
            YamlConfigLoader().store(tmpConfig.toPath(), config)

            val properties = KotlinRadarPropertyHandlerImpl()
            properties.load(tmpConfig.absolutePath)
            return properties
        }

        @Throws(IOException::class)
        fun createBasicConfig(folder: TemporaryFolder): ConfigRadar {
            val config = ConfigRadar()
            config.persistencePath = folder.newFolder().absolutePath
            config.schemaRegistry = emptyList()
            config.broker = emptyList()
            return config
        }

        fun getDisconnectMonitorConfig(port: Int): DisconnectMonitorConfig {
            val disconnectConfig = DisconnectMonitorConfig()
            val notifyConfigs = mutableListOf<NotifyConfig>()
            notifyConfigs.add(NotifyConfig("test", listOf("test@localhost")))
            disconnectConfig.notifyConfig = notifyConfigs
            disconnectConfig.emailHost = "localhost"
            disconnectConfig.emailPort = port
            disconnectConfig.timeout = 1L
            disconnectConfig.alertRepeatInterval = 20L
            return disconnectConfig
        }

        @Throws(IOException::class)
        fun getDisconnectMonitorConfig(port: Int, folder: TemporaryFolder): ConfigRadar {
            val config = createBasicConfig(folder)
            config.disconnectMonitor = getDisconnectMonitorConfig(port)
            return config
        }

        fun getBatteryMonitorConfig(port: Int): BatteryMonitorConfig {
            val batteryConfig = BatteryMonitorConfig()
            val notifyConfigs = mutableListOf<NotifyConfig>()
            notifyConfigs.add(NotifyConfig("test", listOf("test@localhost")))
            batteryConfig.notifyConfig = notifyConfigs
            batteryConfig.emailHost = "localhost"
            batteryConfig.emailPort = port
            batteryConfig.level = "LOW"
            batteryConfig.emailUser = "someuser"
            return batteryConfig
        }

        @Throws(IOException::class)
        fun getBatteryMonitorConfig(port: Int, folder: TemporaryFolder): ConfigRadar {
            val config = createBasicConfig(folder)
            config.batteryMonitor = getBatteryMonitorConfig(port)
            return config
        }

        @Throws(IOException::class)
        fun getSourceStatisticsMonitorConfig(folder: TemporaryFolder): ConfigRadar {
            val config = createBasicConfig(folder)
            val sourceConfig = SourceStatisticsStreamConfig()
            sourceConfig.name = "source_statistics_test"
            sourceConfig.topics = listOf("android_empatica_e4_battery_level", "android_empatica_e4_battery_level_10sec")
            sourceConfig.outputTopic = "statistics_android_empatica_e4"
            sourceConfig.flushTimeout = 200L
            config.statisticsMonitors = listOf(sourceConfig)
            return config
        }
    }
}

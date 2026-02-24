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

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarbase.config.RadarPropertyHandler
import org.radarcns.kafka.ObservationKey
import org.radarbase.util.EmailSenders
import org.radarbase.util.RadarSingletonFactory
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import jakarta.mail.MessagingException

/**
 * Monitors the battery level for any devices running empty. It will optionally notify someone when
 * a battery level is running low and when the battery level has returned to normal again.
 */
class BatteryLevelMonitor(
    radar: RadarPropertyHandler,
    topics: Collection<String>,
    private val senders: EmailSenders?,
    private val minLevel: Status = Status.CRITICAL,
    private val logInterval: Long
) : AbstractKafkaMonitor<GenericRecord, GenericRecord, BatteryLevelMonitor.BatteryLevelState>(
    radar, topics, "battery_monitors", "1", BatteryLevelState()
) {
    private var messageNumber: Long = 0

    init {
        val props = Properties()
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        configure(props)
    }

    override fun evaluateRecord(record: ConsumerRecord<GenericRecord, GenericRecord>) {
        try {
            val key = extractKey(record)
            val batteryLevel = extractBatteryLevel(record)
            val previousLevel = state!!.updateLevel(getStateStore()!!.keyToString(key), batteryLevel)

            if (logInterval > 0 && (messageNumber % logInterval).toInt() == 0) {
                logger.info("Measuring battery level of record offset {} of {} with value {}",
                    record.offset(), key, record.value())
            }
            messageNumber++

            if (batteryLevel <= Status.CRITICAL.level) {
                if (previousLevel > Status.CRITICAL.level) {
                    updateStatus(key, Status.CRITICAL)
                    logger.warn("Battery level of sensor {} of user {} is critically low: {}",
                        key.sourceId, key.userId, record.value())
                }
            } else if (batteryLevel <= Status.LOW.level) {
                if (previousLevel > Status.LOW.level) {
                    updateStatus(key, Status.LOW)
                    logger.warn("Battery level of sensor {} of user {} is low: {}",
                        key.sourceId, key.userId, record.value())
                }
            } else if (previousLevel <= Status.LOW.level) {
                // Remove the email alert for battery monitor for normal level because 
                // it is not crucial and to prevent spamming a user's email account. 
                // Uncomment the line below if needed.
                // updateStatus(key, Status.NORMAL);
                logger.info("Battery of sensor {} of user {} is has returned to normal: {}",
                    key.sourceId, key.userId, record.value())
            }
        } catch (ex: IllegalArgumentException) {
            logger.error("Failed to process record {}", record, ex)
        }
    }

    private fun updateStatus(key: ObservationKey, status: Status) {
        val sender = senders?.getEmailSenderForProject(key.projectId) ?: return

        if (status.level <= minLevel.level) {
            try {
                sender.sendEmail("[RADAR-CNS] battery level low",
                    "The battery level of $key is now $status. Please ensure that it gets recharged.")
                logger.info("Sent battery level status message successfully")
            } catch (mex: MessagingException) {
                logger.error("Failed to send battery level status message.", mex)
            }
        }
        if (status == Status.NORMAL) {
            try {
                sender.sendEmail("[RADAR-CNS] battery level returned to normal",
                    "The battery level of $key has returned to normal. No further action is needed.")
                logger.info("Sent battery level status message successfully")
            } catch (mex: MessagingException) {
                logger.error("Failed to send battery level status message.", mex)
            }
        }
    }

    private fun extractBatteryLevel(record: ConsumerRecord<*, GenericRecord>): Float {
        val value = record.value()
        val batteryField = value.schema.getField("batteryLevel")
            ?: throw IllegalArgumentException("Failed to process record with value type ${value.schema} without batteryLevel field.")
        val batteryLevel = value[batteryField.pos()] as Number
        return batteryLevel.toFloat()
    }

    /** Battery level status. */
    enum class Status(val level: Float) {
        NORMAL(1.0f), LOW(0.2f), CRITICAL(0.05f), EMPTY(0f);
    }

    /** Persist messages that have been sent. */
    class BatteryLevelState {
        var levels: MutableMap<String, Float> = HashMap()

        /** Update a single battery level. */
        fun updateLevel(key: String, level: Float): Float {
            val previousLevel = levels.put(key, level)
            return previousLevel ?: 1.0f
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BatteryLevelMonitor::class.java)

        @JvmStatic
        @Throws(IOException::class)
        fun main(args: Array<String>) {
            val radarPropertyHandler = RadarSingletonFactory.radarPropertyHandler
            radarPropertyHandler.load(null)

            val monitor = BatteryLevelMonitor(radarPropertyHandler,
                listOf("android_empatica_e4_battery_level"), null, Status.CRITICAL, -1)
            monitor.start()
        }
    }
}

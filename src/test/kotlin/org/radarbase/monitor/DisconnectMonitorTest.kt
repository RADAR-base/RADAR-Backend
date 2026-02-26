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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.*
import org.radarbase.util.EmailSender
import org.radarbase.util.EmailSenders
import org.radarbase.util.YamlPersistentStateStore
import org.radarcns.kafka.ObservationKey
import java.nio.file.Path
import java.time.Duration
import java.util.*
import kotlin.test.assertTrue

class DisconnectMonitorTest {

    companion object {

        @TempDir
        lateinit var folder: Path

        private var offset: Long = 0
        private var timeReceived: Long = 0
        private var timesSent: Int = 0
        private lateinit var keySchema: Schema
        private lateinit var valueSchema: Schema
        private lateinit var senders: EmailSenders
        private lateinit var sender: EmailSender

        private val PROJECT_ID = "test"

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val parser = Schema.Parser()
            keySchema = parser.parse(
                "{\"name\": \"key\", \"type\": \"record\", \"fields\": ["
                        + "{\"name\": \"projectId\", \"type\": [\"null\", \"string\"]},"
                        + "{\"name\": \"userId\", \"type\": \"string\"},"
                        + "{\"name\": \"sourceId\", \"type\": \"string\"}"
                        + "]} "
            )

            valueSchema = parser.parse(
                "{\"name\": \"value\", \"type\": \"record\", \"fields\": ["
                        + "{\"name\": \"timeReceived\", \"type\": \"double\"}"
                        + "]} "
            )

            offset = 1000L
            timeReceived = 2000L
            timesSent = 0
            sender = mock(EmailSender::class.java)
            senders = EmailSenders(Collections.singletonMap(PROJECT_ID, sender))
        }
    }

    private fun evaluateRecords() {
        val config = KafkaMonitorFactoryTest.getDisconnectMonitorConfig(25252, folder)

        val disconnectConfig = config.disconnectMonitor!!

        disconnectConfig.timeout = 1L
        disconnectConfig.alertRepeatInterval = 2L
        disconnectConfig.alertRepetitions = 2

        val timeout = Duration.ofSeconds(disconnectConfig.timeout)

        val properties = KafkaMonitorFactoryTest.getRadarPropertyHandler(config, folder)

        val monitor = DisconnectMonitor(properties, listOf("mytopic"), "mygroup", senders)
        monitor.startScheduler()

        assertEquals(timeout, monitor.pollTimeout)

        sendMessage(monitor, "1")
        sendMessage(monitor, "1")
        sendMessage(monitor, "2")
        Thread.sleep(timeout.toMillis() + disconnectConfig.timeout * 1000)
        monitor.evaluateRecords(ConsumerRecords(emptyMap(), emptyMap()))
        timesSent += 2
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString())
        sendMessage(monitor, "1")
        sendMessage(monitor, "1")
        timesSent += 1
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString())
        sendMessage(monitor, "2")
        sendMessage(monitor, "2")
        sendMessage(monitor, "0")
        timesSent += 1
        Thread.sleep(timeout.toMillis() + disconnectConfig.timeout * 1000)
        monitor.evaluateRecords(ConsumerRecords(emptyMap(), emptyMap()))
        timesSent += 3
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString())
    }

    @Test
    fun evaluateRecordsWithScheduledAlerts() {
        evaluateRecords()
        Thread.sleep(7000L)
        timesSent += 6 // executed twice for 3 disconnected devices
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString())
    }

    private fun sendMessage(monitor: DisconnectMonitor, source: String) {
        val key = Record(keySchema)
        key.put("projectId", PROJECT_ID)
        key.put("sourceId", source)
        key.put("userId", "me")

        val value = Record(valueSchema)
        value.put("timeReceived", timeReceived++.toDouble())
        val record = ConsumerRecord<GenericRecord, GenericRecord>("mytopic", 0, offset++, key, value)
        val partition = TopicPartition(record.topic(), record.partition())

        monitor.evaluateRecords(ConsumerRecords(mapOf(partition to listOf(record)), emptyMap()))
    }

    @Test
    fun retrieveState() {
        val base = folder.toFile()
        val stateStore = YamlPersistentStateStore(base)
        val state = DisconnectMonitor.DisconnectMonitorState()
        val key1 = ObservationKey(PROJECT_ID, "a", "b")
        val key2 = ObservationKey(PROJECT_ID, "b", "c")
        val key3 = ObservationKey(PROJECT_ID, "c", "d")
        val now = System.currentTimeMillis()
        state.lastSeen[stateStore.keyToString(key1)] = now
        state.lastSeen[stateStore.keyToString(key2)] = now + 1L
        state.reportedMissing[stateStore.keyToString(key3)] = DisconnectMonitor.MissingRecordsReport(now - 60L, now + 2L, 0)
        stateStore.storeState("one", "two", state)

        val stateStore2 = YamlPersistentStateStore(base)
        val state2 = stateStore2.retrieveState("one", "two", DisconnectMonitor.DisconnectMonitorState())
        val lastSeen = state2.lastSeen
        assertEquals(2, lastSeen.size, "Expected 2 keys in lastSeen")
        assertTrue(lastSeen.containsKey(stateStore.keyToString(key1)))
        assertTrue(lastSeen.containsKey(stateStore.keyToString(key2)))
        assertEquals(lastSeen[stateStore.keyToString(key1)], now)
        assertEquals(lastSeen[stateStore.keyToString(key2)], now + 1L)

        val reported = state2.reportedMissing
        assertEquals(1, reported.size, "Expected 1 key in reportedMissing")
        assertTrue(reported.containsKey(stateStore.keyToString(key3)))
    }
}

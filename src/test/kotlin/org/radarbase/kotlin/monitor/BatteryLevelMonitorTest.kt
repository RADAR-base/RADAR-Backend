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

import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasEntry
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.*
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel
import org.radarbase.util.EmailSender
import org.radarbase.kotlin.util.EmailSenders
import org.radarbase.util.YamlPersistentStateStore
import java.util.*

class BatteryLevelMonitorTest {

    @Rule
    @JvmField
    val folder = TemporaryFolder()

    private var offset: Long = 0
    private var timeReceived: Long = 0
    private var timesSent: Int = 0
    private lateinit var senders: EmailSenders
    private lateinit var sender: EmailSender

    private val PROJECT_ID = "test"

    @Test
    fun evaluateRecord() {
        offset = 1000L
        timeReceived = 2000L
        timesSent = 0
        sender = mock(EmailSender::class.java)

        senders = EmailSenders(Collections.singletonMap(PROJECT_ID, sender))

        val config = KafkaMonitorFactoryTest.getBatteryMonitorConfig(25252, folder)
        val properties = KafkaMonitorFactoryTest.getRadarPropertyHandler(config, folder)

        val monitor = BatteryLevelMonitor(properties, listOf("mytopic"), senders, BatteryLevelMonitor.Status.LOW, 10L)

        sendMessage(monitor, 1.0f, false)
        sendMessage(monitor, 1.0f, false)
        sendMessage(monitor, 0.1f, true)
        sendMessage(monitor, 0.1f, false)
        sendMessage(monitor, 0.3f, false)
        sendMessage(monitor, 0.4f, false)
        sendMessage(monitor, 0.01f, true)
        sendMessage(monitor, 0.01f, false)
        sendMessage(monitor, 0.1f, false)
        sendMessage(monitor, 0.1f, false)
        sendMessage(monitor, 0.01f, true)
        sendMessage(monitor, 1f, false)
    }

    private fun sendMessage(monitor: BatteryLevelMonitor, batteryLevel: Float, sentMessage: Boolean) {
        val key = Record(ObservationKey.getClassSchema())
        key.put("projectId", PROJECT_ID)
        key.put("sourceId", "1")
        key.put("userId", "me")

        val value = Record(EmpaticaE4BatteryLevel.getClassSchema())
        value.put("time", timeReceived.toDouble())
        value.put("timeReceived", timeReceived++.toDouble())
        value.put("batteryLevel", batteryLevel)
        monitor.evaluateRecord(ConsumerRecord("mytopic", 0, offset++, key, value))

        if (sentMessage) {
            timesSent++
        }
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString())
    }

    @Test
    fun retrieveState() {
        val base = folder.newFolder()
        val stateStore = YamlPersistentStateStore(base)
        val state = BatteryLevelMonitor.BatteryLevelState()
        val key1 = ObservationKey("test", "a", "b")
        val keyString = stateStore.keyToString(key1)
        state.updateLevel(keyString, 0.1f)
        stateStore.storeState("one", "two", state)

        val stateStore2 = YamlPersistentStateStore(base)
        val state2 = stateStore2.retrieveState("one", "two", BatteryLevelMonitor.BatteryLevelState())
        val values = state2.levels
        assertThat(values, hasEntry(keyString, 0.1f))
    }
}

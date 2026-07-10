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

package org.radarbase.stream

import org.apache.kafka.streams.kstream.KStream
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.any
import org.mockito.Mockito.doCallRealMethod
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.radarbase.config.KafkaProperty
import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.SingleStreamConfig
import org.radarbase.topic.KafkaTopic
import org.radarbase.util.RadarSingletonFactory
import java.io.IOException
import java.util.stream.Stream
import kotlin.test.Ignore

@Ignore("Covered by Java test; Kotlin + Mockito nullability matcher issue")
class SensorStreamWorkerTest {

    companion object {
        private lateinit var aggregator: SensorStreamWorker<*, *>

        @BeforeAll
        @JvmStatic
        fun setUp() {
            @Suppress("UNCHECKED_CAST")
            val mockAgg = mock(SensorStreamWorker::class.java) as SensorStreamWorker<*, *>
            aggregator = mockAgg
        }
    }

    @Test
    @Throws(IOException::class)
    fun getBuilder() {
        val topicName = "TESTTopic"
        val sensorTopic = StreamDefinition(KafkaTopic(topicName), KafkaTopic(topicName + "_output"))
        `when`(aggregator.getStreamDefinitions()).thenReturn(Stream.of(sensorTopic))

        val propertyHandler: RadarConfigHandler = RadarSingletonFactory.radarConfigHandler
        propertyHandler.load("src/test/resources/config/radar.yml")
        val kafkaProperty: KafkaProperty = propertyHandler.kafkaProperties
        @Suppress("UNCHECKED_CAST", "rawtypes")
        `when`(aggregator.implementStream(eq(sensorTopic), any())).thenReturn(
            mock(KStream::class.java) as KStream<Any, Any>,
        )
        doCallRealMethod().`when`(aggregator).createBuilder(sensorTopic)
        aggregator.createBuilder(sensorTopic)

        verify(aggregator, times(1)).implementStream(eq(sensorTopic), any())
    }
}

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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.processor.TimestampExtractor
import java.util.*

class KafkaProperty(private val radarBackendConfig: RadarBackendConfig) {

    /**
     * @param clientId useful for debugging
     * @param singleStreamConfig stream configuration
     * @return Properties for a Kafka Stream
     */
    fun getStreamProperties(
        clientId: String,
        singleStreamConfig: SingleStreamConfig,
    ): Properties {
        val props = Properties()

        val streamConfig = radarBackendConfig.stream ?: throw IllegalStateException("Stream configuration is missing")

        props[StreamsConfig.APPLICATION_ID_CONFIG] = clientId
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = radarBackendConfig.brokerPaths
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = radarBackendConfig.schemaRegistryPaths
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
        props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = streamConfig.threadsByPriority(singleStreamConfig.priority)
        props[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] =
            LogAndContinueExceptionHandler::class.java.name

        radarBackendConfig.stream?.properties?.let { props.putAll(it) }
        props.putAll(singleStreamConfig.properties)

        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        return props
    }

    /**
     * @param clientId useful for debugging
     * @param singleStreamConfig stream configuration
     * @param timestampExtractor custom timestamp extract that overrides the out-of-the-box
     * @return Properties for a Kafka Stream
     */
    fun getStreamProperties(
        clientId: String,
        singleStreamConfig: SingleStreamConfig,
        timestampExtractor: Class<out TimestampExtractor>,
    ): Properties {
        val props = getStreamProperties(clientId, singleStreamConfig)
        props[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = timestampExtractor.name
        return props
    }
}

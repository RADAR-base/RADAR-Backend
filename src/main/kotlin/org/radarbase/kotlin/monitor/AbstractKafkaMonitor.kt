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

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.WakeupException
import org.radarbase.kotlin.config.RadarPropertyHandler
import org.radarcns.kafka.ObservationKey
import org.radarbase.util.PersistentStateStore
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.*

/**
 * Monitor a list of topics for anomalous behavior.
 * @param <K> record key type
 * @param <V> record value type
 * @param <S> state type
 */
abstract class AbstractKafkaMonitor<K, V, S>(
    radar: RadarPropertyHandler,
    protected val topics: Collection<String>,
    private val groupId: String,
    clientId: String,
    stateDefault: S?
) : KafkaMonitor {
    protected val state: S?
    private val stateStore: PersistentStateStore?
    private val properties: Properties = Properties()
    private val clientId: String

    override var pollTimeout: Duration = Duration.ofDays(365)
    private var consumer: Consumer<K, V>? = null

    private var _isShutdown: Boolean = false
    override val isShutdown: Boolean
        @Synchronized get() = _isShutdown

    init {
        require(topics.isNotEmpty()) { "Cannot start monitor without topics." }

        val deserializer = KafkaAvroDeserializer::class.java.name
        this.clientId = "${javaClass.name}-$clientId"
        properties.apply {
            setProperty(KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
            setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
            setProperty(GROUP_ID_CONFIG, groupId)
            setProperty(CLIENT_ID_CONFIG, this@AbstractKafkaMonitor.clientId)
            setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true")
            setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1001")
            setProperty(SESSION_TIMEOUT_MS_CONFIG, "15101")
            setProperty(HEARTBEAT_INTERVAL_MS_CONFIG, "7500")
        }

        val config = radar.radarProperties
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryPaths)
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.brokerPaths)

        stateStore = try {
            radar.getPersistentStateStore()
        } catch (ex: IOException) {
            logger.warn("Cannot get persistent state store {}. Not persisting state.",
                stateDefault?.let { it::class.java.name } ?: "null", ex)
            null
        }

        var localState = stateDefault
        if (stateStore != null && stateDefault != null) {
            try {
                localState = stateStore.retrieveState(groupId, this.clientId, stateDefault!!)
                logger.info("Using existing {} from persistence store.", stateDefault!!::class.java.name)
            } catch (ex: IOException) {
                logger.warn("Cannot retrieve persistent state {}. Restarting from empty state.",
                    stateDefault!!::class.java.name, ex)
            }
        } else if (stateDefault != null) {
            logger.info("Persistence path not specified; not retrieving or storing state.")
        }
        state = localState
    }

    /**
     * Additional configuration to pass to the consumer.
     */
    protected fun configure(properties: Properties) {
        this.properties.putAll(properties)
    }

    /**
     * Monitor a given topic until the [isShutdown] method returns true.
     *
     * When a message is encountered that cannot be deserialized,
     * [handleSerializationException] is called.
     */
    override fun start() {
        val kafkaConsumer = KafkaConsumer<K, V>(properties)
        consumer = kafkaConsumer
        kafkaConsumer.subscribe(topics)

        logger.info("Monitoring streams {}", topics)

        try {
            while (!isShutdown) {
                try {
                    val records = kafkaConsumer.poll(pollTimeout)
                    evaluateRecords(records)
                } catch (ex: SerializationException) {
                    handleSerializationException()
                } catch (ex: WakeupException) {
                    logger.info("Consumer woke up")
                } catch (ex: InterruptException) {
                    logger.info("Consumer was interrupted")
                    shutdown()
                } catch (ex: KafkaException) {
                    logger.error("Kafka consumer gave exception", ex)
                }
            }
        } finally {
            kafkaConsumer.close()
        }
    }

    /**
     * Handles any deserialization message.
     *
     * This implementation tries to find the partition that contains the faulty message and
     * increases the consumer position to skip that message.
     *
     * The new position is not committed, so on failure of the client, the message must be
     * skipped again.
     */
    protected open fun handleSerializationException() {
        logger.error("Failed to deserialize message. Skipping message.")
        val currentConsumer = consumer ?: return
        topics.parallelStream()
            .flatMap { t -> currentConsumer.partitionsFor(t).stream() }
            .map { tp -> TopicPartition(tp.topic(), tp.partition()) }
            .filter { tp ->
                val tmpProperties = Properties().apply {
                    putAll(properties)
                    setProperty(CLIENT_ID_CONFIG, "${properties.getProperty(CLIENT_ID_CONFIG)}-tmp-${UUID.randomUUID()}")
                }

                try {
                    KafkaConsumer<K, V>(tmpProperties).use { tmpConsumer ->
                        tmpConsumer.assign(listOf(tp))
                        tmpConsumer.seek(tp, currentConsumer.position(tp))
                        tmpConsumer.poll(Duration.ZERO)
                        false
                    }
                } catch (ex: SerializationException) {
                    logger.error("Serialization error, skipping message", ex)
                    true
                }
            }
            .forEach { tp -> currentConsumer.seek(tp, currentConsumer.position(tp) + 1) }
    }

    /** Evaluate a single record that the monitor receives by overriding this function */
    internal abstract fun evaluateRecord(record: ConsumerRecord<K, V>)

    /** Evaluates the records that the monitor receives */
    internal open fun evaluateRecords(records: ConsumerRecords<K, V>) {
        for (record in records) {
            evaluateRecord(record)
        }
        afterEvaluate()
    }

    /** Store the current state. */
    protected open fun storeState() {
        if (stateStore != null && state != null) {
            try {
                stateStore.storeState(groupId, clientId, state)
            } catch (ex: IOException) {
                logger.error("Failed to store monitor state: {}. "
                        + "When restarted, all current state will be lost.", ex.message)
            }
        }
    }

    /** Called after a set of records has been evaluated. */
    protected open fun afterEvaluate() {
        storeState()
    }

    @Synchronized
    override fun shutdown() {
        logger.info("Shutting down monitor {}", javaClass.simpleName)
        _isShutdown = true
        consumer?.wakeup()
    }

    protected fun extractKey(record: ConsumerRecord<GenericRecord, *>): ObservationKey {
        val key = record.key() ?: throw IllegalArgumentException("Failed to process record without a key.")
        return extractKey(key, key.schema)
    }

    protected fun getStateStore(): PersistentStateStore? = stateStore

    companion object {
        private val logger = LoggerFactory.getLogger(AbstractKafkaMonitor::class.java)

        @JvmStatic
        fun extractKey(record: GenericRecord, schema: Schema): ObservationKey {
            val projectIdField = schema.getField("projectId")
                ?: throw IllegalArgumentException("Failed to process record with key type $schema without project ID.")
            val userIdField = schema.getField("userId")
                ?: throw IllegalArgumentException("Failed to process record with key type $schema without user ID.")
            val sourceIdField = schema.getField("sourceId")
                ?: throw IllegalArgumentException("Failed to process record with key type $schema without source ID.")
            
            val projectIdValue = record[projectIdField.pos()]
            return ObservationKey(
                projectIdValue?.toString(),
                record[userIdField.pos()].toString(),
                record[sourceIdField.pos()].toString()
            )
        }
    }
}

package org.radarbase.stream.ruleengine

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.Stores
import org.junit.jupiter.api.Test
import org.radarbase.config.intervention.ConditionConfig
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.processor.RuleGroupProcessor
import org.radarbase.stream.ruleengine.serde.JsonSerde
import org.radarbase.util.getStreamProperties
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.collections.set

class TempTest {
    val ruleGroupSerde = JsonSerde(RuleGroup::class.java)
    val ruleKeySerde = JsonSerde(RuleKey::class.java)
    val interventionConfig = JsonSerde(InterventionConfig::class.java)

    @Test
    fun rawBytesTest() {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "radar-kafka-kafka-bootstrap:9094")
            put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"shared-service-user\" password=\"CRvl4k3Pt1nMBfhQAoIAxdJ4emBpF8mI\";")
            put("sasl.mechanism", "SCRAM-SHA-512")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
            put(ConsumerConfig.GROUP_ID_CONFIG, "temptest-raw-inspect-${System.currentTimeMillis()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
        val consumer = org.apache.kafka.clients.consumer.KafkaConsumer<ByteArray, ByteArray>(props)
        consumer.use {
            val topic = "rule_engine_config"
            val partitions = consumer.partitionsFor(topic).map { org.apache.kafka.common.TopicPartition(topic, it.partition()) }
            consumer.assign(partitions)
            consumer.seekToBeginning(partitions)
            var seen = 0
            val deadline = System.currentTimeMillis() + 15_000
            while (seen < 27 && System.currentTimeMillis() < deadline) {
                val records = consumer.poll(java.time.Duration.ofMillis(1000))
                for (record in records) {
                    seen++
                    val keyBytes = record.key()
                    val valueBytes = record.value()
                    println("--- partition=${record.partition()} offset=${record.offset()} ---")
                    println("  key  (${keyBytes?.size ?: 0} bytes) hex=${keyBytes?.take(16)?.joinToString(" ") { "%02x".format(it) }}")
                    println("  key  as utf8 (lossy) = ${keyBytes?.let { String(it, Charsets.UTF_8) }}")
                    println("  value(${valueBytes?.size ?: 0} bytes) hex=${valueBytes?.take(16)?.joinToString(" ") { "%02x".format(it) }}")
                    println("  value as utf8 (lossy) = ${valueBytes?.let { String(it, Charsets.UTF_8) }?.take(120)}")
                    if (valueBytes != null && valueBytes.isNotEmpty()) {
                        println("  value byte[0] (magic byte check, 0x00 = Confluent Avro wire format) = ${valueBytes[0]}")
                    }
                }
            }
            println("=== Total raw records inspected: $seen ===")
        }
    }

    @Test
    fun tempTest() {
            val props = Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "org.radarbase.stream.ruleengine.RuleEngineStream-0.5.0")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "radar-kafka-kafka-bootstrap:9094")
                put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"shared-service-user\" password=\"CRvl4k3Pt1nMBfhQAoIAxdJ4emBpF8mI\";")
                put("sasl.mechanism", "SCRAM-SHA-512")
            }
            val storeName = "JIT_RULES_STORE"
            // Wired up exactly like the real RuleEngineStream.createStreams(): typed JsonSerdes for the
            // store value and for the global-store topic Consumed, and the REAL RuleGroupProcessor
            // (not the no-op RuleGroupProcessor2) so we can see whether scope-key aggregation actually
            // runs against historical (restored) records, not just live ones.
            val storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.String(),
                ruleGroupSerde,
            )
            val builder = StreamsBuilder()
            builder.addGlobalStore(
                storeBuilder,
                "rule_engine_config",
                Consumed.with(ruleKeySerde, interventionConfig)
                    .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST),
                ProcessorSupplier { RuleGroupProcessor(storeName) },
            )

            val streams = KafkaStreams(builder.build(), props)

            streams.setGlobalStateRestoreListener(object : org.apache.kafka.streams.processor.StateRestoreListener {
                override fun onRestoreStart(
                    topicPartition: org.apache.kafka.common.TopicPartition,
                    storeName: String,
                    startingOffset: Long,
                    endingOffset: Long
                ) {
                    println("Restoring $storeName from partition ${topicPartition.partition()} starting at offset $startingOffset up to $endingOffset")
                }
                override fun onBatchRestored(topicPartition: org.apache.kafka.common.TopicPartition, storeName: String, batchEndOffset: Long, numRestored: Long) {
                    println("Batch restoring $storeName from partition ${topicPartition.partition()} till $batchEndOffset and a total of $numRestored restored")
                }
                override fun onRestoreEnd(topicPartition: org.apache.kafka.common.TopicPartition, storeName: String, totalRestored: Long) {
                    println("Finished restoring $storeName. Restored $totalRestored records.")
                }
            })

            val dumped = CountDownLatch(1)
            streams.setStateListener { newState, _ ->
                if (newState == KafkaStreams.State.RUNNING) {
                    try {
                        println("=== Global store is RUNNING, dumping contents of $storeName ===")
                        val store = streams.store(
                            StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore<String, RuleGroup>()),
                        )
                        var count = 0
                        val iterator: KeyValueIterator<String, RuleGroup> = store.all()
                        iterator.use {
                            while (it.hasNext()) {
                                val kv: KeyValue<String, RuleGroup> = it.next()
                                count++
                                println("  key=${kv.key} value=${kv.value}")
                            }
                        }
                        println("=== Total entries readable as RuleGroup: $count ===")
                    } catch (e: Exception) {
                        println("=== Failed to iterate store as RuleGroup: ${e.javaClass.simpleName}: ${e.message} ===")
                    } finally {
                        dumped.countDown()
                    }
                }
            }

            //streams.cleanUp()
            streams.start()
            dumped.await(30, TimeUnit.SECONDS)
            streams.close()
    }
}


class RuleGroupProcessor2(
    val globalStoreName: String,
) : Processor<String, String, Void, Void> {

    private lateinit var store: KeyValueStore<String, RuleGroup>

    override fun init(context: ProcessorContext<Void, Void>) {
        store = context.getStateStore(globalStoreName)
    }

    override fun process(record: Record<String, String>) {
    }
}

/**
 * Wraps [Serdes.String] so every (de)serialization call is logged, to see exactly when/how
 * often the global store's key and value serdes get invoked.
 */
class LoggingStringSerde : Serde<String> {
    override fun serializer(): Serializer<String> = LoggingStringSerializer()
    override fun deserializer(): Deserializer<String> = LoggingStringDeserializer()
}

class LoggingStringSerializer : Serializer<String> {
    private val delegate = Serdes.String().serializer()

    override fun serialize(topic: String, data: String?): ByteArray? {
        println("[LoggingStringSerde] serialize   topic=$topic data=$data")
        return delegate.serialize(topic, data)
    }
}

class LoggingStringDeserializer : Deserializer<String> {
    private val delegate = Serdes.String().deserializer()

    override fun deserialize(topic: String, data: ByteArray?): String? {
        val result = delegate.deserialize(topic, data)
        println("[LoggingStringSerde] deserialize topic=$topic result=$result")
        return result
    }
}
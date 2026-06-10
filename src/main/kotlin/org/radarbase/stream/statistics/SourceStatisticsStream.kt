package org.radarbase.stream.statistics

import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.Cancellable
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.ProcessorSupplier
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.radarbase.config.SourceStatisticsStreamConfig
import org.radarbase.monitor.AbstractKafkaMonitor
import org.radarbase.stream.AbstractStreamWorker
import org.radarbase.util.serde.RadarSerde
import org.radarcns.kafka.ObservationKey
import org.radarcns.stream.SourceStatistics
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import kotlin.math.max
import kotlin.math.min

class SourceStatisticsStream : AbstractStreamWorker() {
    private var streamName: String? = null
    private var interval: Duration = Duration.ZERO

    override fun createStreams(): List<KafkaStreams> {
        return listOf(KafkaStreams(topology, streamsConfig))
    }

    override fun doCleanup() {
        // do nothing
    }

    override fun initialize() {
        val config = this.config as SourceStatisticsStreamConfig
        this.streamName = config.name

        val inputTopics = config.topics
        require(!(inputTopics == null || inputTopics.isEmpty())) { "Input topics for stream $streamName is empty" }
        requireNotNull(config.outputTopic) { "Output topic for stream $streamName is missing" }

        inputTopics.forEach { t -> defineStream(t, config.outputTopic) }
        this.interval = Duration.ofMillis(config.flushTimeout)
    }

    private val topology: Topology
        get() {
            val builder = Topology()
            val genericReader = GenericAvroDeserializer()

            val statisticsStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("statistics"),
                SpecificAvroSerde<ObservationKey>(),
                RadarSerde(SourceStatisticsRecord::class.java).getSerde(),
            )

            val inputTopics = streamDefinitions.map { it.inputTopic.name }.toTypedArray()

            builder.addSource("source", genericReader, genericReader, *inputTopics)
            builder.addProcessor(
                "process",
                ProcessorSupplier { SourceStatisticsProcessor() },
                "source",
            )
            builder.addSink(
                "sink",
                streamDefinitions.firstNotNullOfOrNull { it.outputTopic.name }
                    ?: throw IllegalStateException("Output topic for SourceStatisticsStream $streamName is undefined."),
                SpecificAvroSerializer<ObservationKey>(),
                SpecificAvroSerializer<SourceStatistics>(),
                "process",
            )

            builder.addStateStore(statisticsStore, "process")
            return builder
        }

    private val streamsConfig: Properties
        get() {
            val settings = kafkaProperty.getStreamProperties(streamName!!, config)
            settings.remove(DEFAULT_KEY_SERDE_CLASS_CONFIG)
            settings.remove(DEFAULT_VALUE_SERDE_CLASS_CONFIG)
            return settings
        }

    private inner class SourceStatisticsProcessor :
        Processor<GenericRecord, GenericRecord> {
        private lateinit var context: ProcessorContext
        private lateinit var store: KeyValueStore<ObservationKey, SourceStatisticsRecord>
        private var punctuateCancellor: Cancellable? = null
        private var localInterval = Duration.ZERO

        @Suppress("UNCHECKED_CAST")
        override fun init(
            context: ProcessorContext,
        ) {
            store = context.getStateStore("statistics") as KeyValueStore<ObservationKey, SourceStatisticsRecord>
            this.context = context
            updatePunctuate()
        }

        private fun updatePunctuate() {
            if (localInterval != interval) {
                localInterval = interval
                punctuateCancellor?.cancel()
                punctuateCancellor = this.context.schedule(
                    Duration.ofMillis(localInterval.toMillis()),
                    PunctuationType.WALL_CLOCK_TIME,
                ) { this.sendNew() }
            }
        }

        private fun sendNew() {
            val sent = mutableListOf<KeyValue<ObservationKey, SourceStatisticsRecord>>()

            store.all().use { iterator ->
                while (iterator.hasNext()) {
                    val next = iterator.next()
                    if (!next.value.isSent) {
                        context.forward(next.key, next.value)
                        sent.add(KeyValue(next.key, next.value.sentRecord()))
                    }
                }
            }

            sent.forEach { e -> store.put(e.key, e.value) }
            context.commit()

            updatePunctuate()
        }

        override fun process(key: GenericRecord?, value: GenericRecord?) {
            if (key == null || value == null) {
                logger.error("Cannot process records without both a key and a value")
                return
            }
            val keySchema = key.schema
            val valueSchema = value.schema

            var time = getTime(value, valueSchema, "time", Double.NaN)
            time = getTime(value, valueSchema, "timeReceived", time)
            val timeStart = getTime(key, keySchema, "timeStart", time)
            val timeEnd = getTime(key, keySchema, "timeEnd", time)

            if (timeStart.isNaN() || timeEnd.isNaN()) {
                logger.error("Record did not contain time values: <{}, {}>", key, value)
                return
            }

            val key: ObservationKey = try {
                AbstractKafkaMonitor.Companion.extractKey(key, keySchema)
            } catch (ex: IllegalArgumentException) {
                logger.error("Could not deserialize key without projectId, userId or sourceId: {}", key)
                return
            }

            val stats = store.get(key)
            val newStats = SourceStatisticsRecord.updateRecord(stats, timeStart, timeEnd)
            if (newStats != stats) {
                store.put(key, newStats)
            }
        }

        override fun close() {
            // do nothing
        }
    }

    data class SourceStatisticsRecord(
        val timeStart: Double,
        val timeEnd: Double,
        val isSent: Boolean,
    ) {
        fun sourceStatistics() = SourceStatistics(timeStart, timeEnd)

        fun sentRecord() = copy(isSent = true)

        companion object {
            fun updateRecord(old: SourceStatisticsRecord?, timeStart: Double, timeEnd: Double): SourceStatisticsRecord {
                return if (old == null) {
                    SourceStatisticsRecord(timeStart, timeEnd, false)
                } else if (old.timeStart > timeStart || old.timeEnd < timeEnd) {
                    SourceStatisticsRecord(
                        min(timeStart, old.timeStart),
                        max(timeEnd, old.timeEnd),
                        false,
                    )
                } else {
                    old
                }
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SourceStatisticsStream::class.java)

        private fun getTime(record: GenericRecord, schema: Schema, fieldName: String, defaultValue: Double): Double {
            val field = schema.getField(fieldName)
            return if (field != null) {
                (record.get(field.pos()) as Number).toDouble()
            } else {
                defaultValue
            }
        }
    }
}

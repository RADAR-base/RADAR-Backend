package org.radarbase.kotlin.stream

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.radarbase.kotlin.util.Monitor
import org.radarbase.kotlin.util.RadarSingletonFactory
import org.radarbase.kotlin.util.RadarUtilities
import org.radarbase.kotlin.util.StreamUtil
import org.radarbase.kotlin.util.serde.RadarSerdes
import org.radarbase.stream.collector.AggregateListCollector
import org.radarbase.stream.collector.NumericAggregateCollector
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.stream.aggregator.AggregateList
import org.radarcns.stream.aggregator.NumericAggregate
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ThreadLocalRandom
import java.util.stream.Collectors

abstract class SensorStreamWorker<K : SpecificRecord, V : SpecificRecord> : AbstractStreamWorker() {
    private val monitorLog = LoggerFactory.getLogger(javaClass)
    private var monitors: MutableCollection<ScheduledFuture<*>>? = null

    protected val utilities: RadarUtilities = RadarSingletonFactory.radarUtilities

    init {
        this.streams = null
        this.monitors = mutableListOf<ScheduledFuture<*>>()
    }

    internal fun createBuilder(def: StreamDefinition): KeyValue<ScheduledFuture<*>?, KafkaStreams> {
        val monitor: Monitor?
        val future: ScheduledFuture<*>?
        if (monitorLog != null) {
            monitor = Monitor(monitorLog, "records have been read from ${def.inputTopic} to ${def.outputTopic}")
            future = master.addMonitor(monitor as org.radarbase.util.Monitor)
        } else {
            monitor = null
            future = null
        }

        val builder = StreamsBuilder()

        val kstream = implementStream(
            def,
            builder.stream<K, V>(def.inputTopic.name)
                .map { k, v ->
                    monitor?.increment()
                    KeyValue.pair(k, v)
                }
        )

        val outputTopicName = def.outputTopic.name
        kstream.to(outputTopicName)

        val properties = getStreamProperties(def)
        return KeyValue.pair(future, KafkaStreams(builder.build(), properties))
    }

    internal fun getStreamProperties(definition: StreamDefinition): Properties {
        val localClientId = buildString {
            append(javaClass.name)
            append("-")
            append(allConfig.buildVersion)
            definition.timeWindows?.let {
                append("-")
                append(it.sizeMs)
                append("-")
                append(it.advanceMs)
            }
        }

        val props = kafkaProperty.getStreamProperties(
            localClientId,
            config,
            DeviceTimestampExtractor::class.java
        )

        val interval = (ThreadLocalRandom.current().nextDouble(0.75, 1.25) *
                definition.commitInterval.toMillis()).toLong()

        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = interval.toString()

        return props
    }

    internal abstract fun implementStream(definition: StreamDefinition, kstream: KStream<K, V>): KStream<*, *>

    override fun createStreams(): List<KafkaStreams>? {
        val streamBuilders = getStreamDefinitions()
            .map { createBuilder(it) }
            .collect(Collectors.toList())

        monitors = streamBuilders.stream()
            .map(StreamUtil.first())
            .filter { it != null }
            .map { it!! }
            .collect(Collectors.toList())

        return streamBuilders.stream()
            .map(StreamUtil.second())
            .collect(Collectors.toList())
    }

    override fun doCleanup() {
        monitors?.forEach { it.cancel(false) }
        monitors = null
    }

    protected fun aggregateNumeric(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, V>,
        fieldName: String,
        schema: Schema
    ): KStream<AggregateKey, NumericAggregate> {
        return kstream.groupByKey()
            .windowedBy(definition.timeWindows)
            .aggregate(
                { NumericAggregateCollector(fieldName, schema) },
                { _, v, valueCollector -> valueCollector.add(v) },
                RadarSerdes.materialized(
                    definition.stateStoreName,
                    RadarSerdes.getInstance().getNumericAggregateCollector() as org.apache.kafka.common.serialization.Serde<NumericAggregateCollector>
                )
            )
            .toStream()
            .map(utilities::numericCollectorToAvro)
    }

    protected fun aggregateCustomNumeric(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, V>,
        calculation: (V) -> Double,
        fieldName: String
    ): KStream<AggregateKey, NumericAggregate> {
        return kstream.groupByKey()
            .windowedBy(definition.timeWindows)
            .aggregate(
                { NumericAggregateCollector(fieldName) },
                { _, v, valueCollector -> valueCollector.add(calculation(v)) },
                RadarSerdes.materialized(
                    definition.stateStoreName,
                    RadarSerdes.getInstance().getNumericAggregateCollector() as org.apache.kafka.common.serialization.Serde<NumericAggregateCollector>
                )
            )
            .toStream()
            .map(utilities::numericCollectorToAvro)
    }

    protected fun aggregateFields(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, V>,
        fieldNames: Array<String>,
        schema: Schema
    ): KStream<AggregateKey, AggregateList> {
        return kstream.groupByKey()
            .windowedBy(definition.timeWindows)
            .aggregate(
                { AggregateListCollector(fieldNames, schema, false) },
                { _, v, valueCollector -> valueCollector.add(v) },
                RadarSerdes.materialized(
                    definition.stateStoreName,
                    RadarSerdes.getInstance().getAggregateListCollector() as org.apache.kafka.common.serialization.Serde<AggregateListCollector>
                )
            )
            .toStream()
            .map(utilities::listCollectorToAvro)
    }

    override fun toString(): String {
        return javaClass.simpleName
    }
}

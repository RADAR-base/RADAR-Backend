package org.radarbase.util

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TimestampExtractor
import org.radarbase.config.KafkaProperty
import org.radarbase.config.RadarBackendConfig
import org.radarbase.config.SingleStreamConfig
import org.radarbase.stream.StreamDefinition
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.stream.Stream

object StreamUtil {
    fun <K, V> test(bip: (K, V) -> Boolean): (KeyValue<K, V>) -> Boolean = { entry -> bip(entry.key, entry.value) }

    fun <K, V, R> apply(bif: (K, V) -> R): (KeyValue<K, V>) -> R = { entry -> bif(entry.key, entry.value) }

    fun <K, V> first(): (KeyValue<K, V>) -> K = { it.key }
    fun <K, V> second(): (KeyValue<K, V>) -> V = { it.value }

    fun interface StreamSupplier<T> {
        fun get(): Stream<T>

        fun concat(other: StreamSupplier<out T>): StreamSupplier<T> =
            StreamSupplier { Stream.concat(get(), other.get()) }

        companion object {
            fun <T> supply(supplier: StreamSupplier<T>): StreamSupplier<T> = supplier
        }
    }
}

internal fun getStreamProperties(
    clazz: Class<*>,
    definition: StreamDefinition,
    streamConfig: SingleStreamConfig,
    config: RadarBackendConfig,
    kafkaProperty: KafkaProperty,
    timeStampExtractorClass: Class<out TimestampExtractor>? = null,
): Properties {
    val localClientId = buildString {
        append(clazz.name)
        append("-")
        append(config.buildVersion)
        definition.timeWindows?.let {
            append("-")
            append(it.sizeMs)
            append("-")
            append(it.advanceMs)
        }
    }

    val props = if (timeStampExtractorClass!=null) kafkaProperty.getStreamProperties(
        localClientId,
        streamConfig,
        timeStampExtractorClass,
    )
    else kafkaProperty.getStreamProperties(
        localClientId,
        streamConfig,
    )

    val interval = (ThreadLocalRandom.current().nextDouble(0.75, 1.25) * definition.commitInterval.toMillis()).toLong()

    props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = interval.toString()

    return props
}

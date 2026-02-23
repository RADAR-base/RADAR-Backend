package org.radarbase.kotlin.util

import org.apache.kafka.streams.KeyValue
import java.util.stream.Stream

object StreamUtil {
    fun <K, V> test(bip: (K, V) -> Boolean): (KeyValue<K, V>) -> Boolean =
        { entry -> bip(entry.key, entry.value) }

    fun <K, V, R> apply(bif: (K, V) -> R): (KeyValue<K, V>) -> R =
        { entry -> bif(entry.key, entry.value) }

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

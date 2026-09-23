package org.radarbase.stream.ruleengine.harness

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.time.Duration
import java.time.Instant
import java.util.Properties

/**
 * Polls a raw `ByteArray`-valued output topic until at least [minCount] records have been
 * decoded, instead of a single blind read right after the stream reaches RUNNING.
 */
object OutputTopicConsumer {
    private val POLL_INTERVAL: Duration = Duration.ofMillis(200)

    fun <V> waitForRecordValues(
        bootstrapServers: String,
        topic: String,
        minCount: Int,
        timeout: Duration = Duration.ofSeconds(30),
        decodeValue: (ByteArray) -> V,
    ): List<V> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "it-$topic-${System.nanoTime()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }
        KafkaConsumer(props, ByteArrayDeserializer(), ByteArrayDeserializer()).use { consumer ->
            consumer.subscribe(listOf(topic))
            val values = mutableListOf<V>()
            val deadline = Instant.now().plus(timeout)
            while (values.size < minCount && Instant.now().isBefore(deadline)) {
                val records = consumer.poll(POLL_INTERVAL)
                records.forEach { record -> values += decodeValue(record.value()) }
            }
            return values
        }
    }
}

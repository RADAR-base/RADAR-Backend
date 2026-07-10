package org.radarbase.monitor

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.junit.Assert
import org.radarbase.config.RadarConfigHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*

class E4AggregatedAccelerationMonitor(
    radar: RadarConfigHandler,
    topic: String?,
    clientID: String
) : AbstractKafkaMonitor<GenericRecord?, GenericRecord?, Any?>(
    radar,
    mutableListOf(topic!!),
    "new",
    clientID,
null
) {
    init {
        val props = Properties().apply {
            this.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            this.putAll(radar.radarProperties.stream!!.properties!!)
        }
        configure(props)
    }

    override fun evaluateRecord(record: ConsumerRecord<GenericRecord?, GenericRecord?>) {
        TODO("Not yet implemented")
    }

    override fun evaluateRecords(records: ConsumerRecords<GenericRecord?, GenericRecord?>) {
        records.forEach {
            val key = it.key() ?: run {
                logger.error("Failed to process record {} without a key.", it)
                return
            }
            val keySchema = key.schema
            if (keySchema.getField("userId") != null && keySchema.getField("sourceId") != null) {
                Assert.assertNotNull(key.get("userId"))
                Assert.assertNotNull(key.get("sourceId"))
            } else {
                logger.error("Failed to process record {} with wrong key type {}.", it, key.schema)
                return
            }
            val value: GenericRecord = it.value()!!
            val fields = value.get("fields") as GenericData.Array<*>
            logger.info("Received [{}, {}, {}] E4 messages",
                (fields[0] as GenericRecord).get("count"),
                (fields[1] as GenericRecord).get("count"),
                (fields[2] as GenericRecord).get("count")
            )

            if (((fields[0] as GenericRecord).get("count") as Int?)!! > 100) {
                shutdown()
            }
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(E4AggregatedAccelerationMonitor::class.java)
    }
}

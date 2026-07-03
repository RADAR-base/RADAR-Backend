package org.radarbase.stream

import org.apache.avro.AvroRuntimeException
import org.apache.avro.generic.IndexedRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import org.slf4j.LoggerFactory

/**
 * Custom Kafka Streams [TimestampExtractor] that extracts the timestamp from the `timeReceived` field
 * of an Avro record. The `timeReceived` field is expected to be a Double representing seconds since epoch.
 */
class DeviceTimestampExtractor : TimestampExtractor {
    override fun extract(record: ConsumerRecord<Any, Any>, previousTimestamp: Long): Long {
        val value = record.value() as IndexedRecord
        val recordSchema = value.schema

        try {
            val field = recordSchema.getField("timeReceived")
            val fieldValue = value.get(field.pos())
            if (fieldValue is Double) {
                return (1000.0 * fieldValue).toLong()
            } else {
                logger.error("timeReceived is not a Double in {}", record)
            }
        } catch (e: AvroRuntimeException) {
            logger.error("Cannot extract timeReceived from {}", record, e)
        }

        throw RuntimeException("Impossible to extract timeReceived from $record")
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DeviceTimestampExtractor::class.java)
    }
}

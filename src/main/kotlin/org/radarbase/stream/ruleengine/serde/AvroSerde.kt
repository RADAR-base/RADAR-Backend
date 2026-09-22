package org.radarbase.stream.ruleengine.serde

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory

class AvroSerde(private val schema: Schema) {

    fun decode(data: ByteArray): GenericRecord {
        val reader = GenericDatumReader<GenericRecord>(schema)
        val decoder = DecoderFactory.get().binaryDecoder(
            data,
            CONFLUENT_WIRE_FORMAT_HEADER_SIZE,
            data.size - CONFLUENT_WIRE_FORMAT_HEADER_SIZE,
            null,
        )
        return reader.read(null, decoder)
    }

    fun unsupportedProducer(topic: String): Nothing =
        throw UnsupportedOperationException(
            "rule_engine_config is an externally-owned config topic, this app only ever consumes it (topic=$topic)",
        )

    companion object {
        private const val CONFLUENT_WIRE_FORMAT_HEADER_SIZE = 5
    }
}

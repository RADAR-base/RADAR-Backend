package org.radarbase.stream.ruleengine.serde

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.slf4j.LoggerFactory


private const val CONFLUENT_WIRE_FORMAT_HEADER_SIZE = 5

private val KEY_SCHEMA: Schema = Schema.Parser().parse(
    """
    {
      "type": "record",
      "name": "AppConfigKey",
      "namespace": "org.radarbase.stream.ruleengine.avro",
      "fields": [
        {"name": "client_id", "type": ["null", "string"], "default": null},
        {"name": "scope", "type": ["null", "string"], "default": null},
        {"name": "name", "type": ["null", "string"], "default": null}
      ]
    }
    """.trimIndent(),
)

private val VALUE_SCHEMA: Schema = Schema.Parser().parse(
    """
    {
      "type": "record",
      "name": "AppConfigRow",
      "namespace": "org.radarbase.stream.ruleengine.avro",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "client_id", "type": ["null", "string"], "default": null},
        {"name": "scope", "type": ["null", "string"], "default": null},
        {"name": "name", "type": ["null", "string"], "default": null},
        {"name": "value", "type": ["null", "string"], "default": null},
        {"name": "create_timestamp", "type": ["null", "long"], "default": null},
        {"name": "version", "type": ["null", "int"], "default": null}
      ]
    }
    """.trimIndent(),
)

private fun decodeGenericRecord(schema: Schema, data: ByteArray): GenericRecord {
    val reader = GenericDatumReader<GenericRecord>(schema)
    val decoder = DecoderFactory.get().binaryDecoder(
        data,
        CONFLUENT_WIRE_FORMAT_HEADER_SIZE,
        data.size - CONFLUENT_WIRE_FORMAT_HEADER_SIZE,
        null,
    )
    return reader.read(null, decoder)
}

private fun unsupportedProducer(topic: String): Nothing =
    throw UnsupportedOperationException(
        "rule_engine_config is an externally-owned config topic, this app only ever consumes it (topic=$topic)",
    )

class RuleKeyAvroSerde : Serde<RuleKey> {
    override fun serializer(): Serializer<RuleKey> = Serializer { topic, _ -> unsupportedProducer(topic) }

    override fun deserializer(): Deserializer<RuleKey> = Deserializer { _, data ->
        data?.let {
            val record = decodeGenericRecord(KEY_SCHEMA, it)
            RuleKey(
                clientId = record["client_id"]?.toString().orEmpty(),
                scope = record["scope"]?.toString().orEmpty(),
                name = record["name"]?.toString().orEmpty(),
            )
        }
    }
}

class InterventionConfigAvroSerde : Serde<InterventionConfig> {
    private val mapper = jacksonObjectMapper()

    override fun serializer(): Serializer<InterventionConfig> = Serializer { topic, _ -> unsupportedProducer(topic) }

    override fun deserializer(): Deserializer<InterventionConfig> = object : Deserializer<InterventionConfig> {
        override fun deserialize(topic: String?, data: ByteArray?): InterventionConfig? {
            val bytes = data ?: return null
            val record = decodeGenericRecord(VALUE_SCHEMA, bytes)
            val json = record["value"]?.toString()
            if (json == null) {
                logger.debug(
                    "rule_engine_config row id={} client_id={} scope={} name={} has no 'value' payload, treating as tombstone",
                    record["id"],
                    record["client_id"],
                    record["scope"],
                    record["name"],
                )
                return null
            }
            return try {
                mapper.readValue(json, InterventionConfig::class.java)
            } catch (e: JsonProcessingException) {
                logger.warn(
                    "Failed to parse InterventionConfig JSON for rule_engine_config row id={} client_id={} scope={} name={}: {}",
                    record["id"],
                    record["client_id"],
                    record["scope"],
                    record["name"],
                    e.message,
                )
                null
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(InterventionConfigAvroSerde::class.java)
    }
}

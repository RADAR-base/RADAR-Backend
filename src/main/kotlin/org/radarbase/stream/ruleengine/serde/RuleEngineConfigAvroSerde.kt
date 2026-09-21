package org.radarbase.stream.ruleengine.serde

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

/**
 * The rule_engine_config topic is produced by an external app-config service as Confluent
 * wire-format Avro (5-byte magic-byte + schema-ID prefix). Its schema registry isn't reachable
 * from this repo (only the Kafka broker is), so these schemas were reverse-engineered directly
 * from the raw bytes instead of fetched from a registry - see
 * docs/rule-engine-global-store-findings.md and src/main/avro/rule_engine_config_*.avsc for the
 * evidence and field-by-field derivation. Every record is a client_id/scope/name-keyed config
 * row whose "value" column holds a JSON-encoded [InterventionConfig], not a nested Avro
 * structure.
 */
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

    override fun deserializer(): Deserializer<InterventionConfig> = Deserializer { _, data ->
        data?.let {
            val record = decodeGenericRecord(VALUE_SCHEMA, it)
            val json = record["value"]?.toString()
                ?: throw IllegalStateException("rule_engine_config row id=${record["id"]} has no 'value' payload")
            mapper.readValue(json, InterventionConfig::class.java)
        }
    }
}

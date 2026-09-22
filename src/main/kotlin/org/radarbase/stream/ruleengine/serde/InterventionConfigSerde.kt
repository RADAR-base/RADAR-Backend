package org.radarbase.stream.ruleengine.serde

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.radarbase.config.intervention.InterventionConfig
import org.slf4j.LoggerFactory

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

class InterventionConfigSerde : Serde<InterventionConfig> {
    private val avroSerde = AvroSerde(VALUE_SCHEMA)
    private val mapper = jacksonObjectMapper()

    override fun serializer(): Serializer<InterventionConfig> = Serializer { topic, _ -> avroSerde.unsupportedProducer(topic) }

    override fun deserializer(): Deserializer<InterventionConfig> = object : Deserializer<InterventionConfig> {
        override fun deserialize(topic: String?, data: ByteArray?): InterventionConfig? {
            val bytes = data ?: return null
            val record = avroSerde.decode(bytes)
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
        private val logger = LoggerFactory.getLogger(InterventionConfigSerde::class.java)
    }
}

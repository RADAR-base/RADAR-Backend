package org.radarbase.stream.ruleengine.harness

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.radarbase.config.intervention.InterventionConfig
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.Properties

/**
 * Encodes and publishes fixture rows for the `rule_engine_config` topic in the same Confluent
 * wire format (1 magic byte + 4-byte schema-ID + Avro binary) that
 * [org.radarbase.stream.ruleengine.serde.RuleEngineConfigAvroSerde] decodes. That production serde
 * has no working serializer for this topic - it's externally owned, see its doc comment - so this
 * encoder is test-only and mirrors its reverse-engineered AppConfigKey/AppConfigRow schemas; it
 * must not be promoted into main as a production serializer.
 */
object RuleEngineConfigFixtures {
    private const val WIRE_FORMAT_MAGIC_BYTE = 0
    private const val FIXTURE_SCHEMA_ID = 1

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

    private val mapper = jacksonObjectMapper()

    private fun encode(schema: Schema, record: GenericData.Record): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(WIRE_FORMAT_MAGIC_BYTE)
        out.write(ByteBuffer.allocate(4).putInt(FIXTURE_SCHEMA_ID).array())
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        GenericDatumWriter<GenericData.Record>(schema).write(record, encoder)
        encoder.flush()
        return out.toByteArray()
    }

    fun key(clientId: String, scope: String, name: String): ByteArray = encode(
        KEY_SCHEMA,
        GenericData.Record(KEY_SCHEMA).apply {
            put("client_id", clientId)
            put("scope", scope)
            put("name", name)
        },
    )

    fun value(id: Int, clientId: String, scope: String, name: String, config: InterventionConfig): ByteArray = encode(
        VALUE_SCHEMA,
        GenericData.Record(VALUE_SCHEMA).apply {
            put("id", id)
            put("client_id", clientId)
            put("scope", scope)
            put("name", name)
            put("value", mapper.writeValueAsString(config))
            put("create_timestamp", System.currentTimeMillis())
            put("version", 1)
        },
    )

    fun publish(bootstrapServers: String, topic: String, key: ByteArray, value: ByteArray) {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        }
        KafkaProducer(props, ByteArraySerializer(), ByteArraySerializer()).use { producer ->
            producer.send(ProducerRecord(topic, key, value)).get()
        }
    }
}

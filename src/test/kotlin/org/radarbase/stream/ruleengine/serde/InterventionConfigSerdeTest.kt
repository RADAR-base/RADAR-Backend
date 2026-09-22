package org.radarbase.stream.ruleengine.serde

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class InterventionConfigSerdeTest {

    private val serde = InterventionConfigSerde()

    @Test
    fun `deserializer returns null instead of throwing for malformed legacy JSON`() {
        val malformedJson = """
            {"name":"rule4","topic":"output_topic","conditions":[{"type":"cel","expression":"value.answers.exists(a, a.questionId == 'q1')"       "projects":["STAGING_PROJECT"],"subjects":null,"name":"cond1"}],"actions":[{"name":"notify"}]}
        """.trimIndent()
        val bytes = encodeRow(id = 31, clientId = "app-config", scope = "project.test-questionnaire", name = "rule4", valueJson = malformedJson)

        val result = serde.deserializer().deserialize("rule_engine_config", bytes)

        assertNull(result)
    }

    @Test
    fun `deserializer returns null when the value column is absent`() {
        val bytes = encodeRow(id = 1, clientId = "app-config", scope = "global", name = "rule1", valueJson = null)

        val result = serde.deserializer().deserialize("rule_engine_config", bytes)

        assertNull(result)
    }

    @Test
    fun `deserializer parses well-formed JSON`() {
        val json = """{"name":"rule1","topic":"output_topic","conditions":[],"actions":[]}"""
        val bytes = encodeRow(id = 2, clientId = "app-config", scope = "global", name = "rule1", valueJson = json)

        val result = serde.deserializer().deserialize("rule_engine_config", bytes)

        assertNotNull(result)
        assertEquals("rule1", result.name)
    }

    private fun encodeRow(id: Int, clientId: String, scope: String, name: String, valueJson: String?): ByteArray {
        val record = GenericData.Record(VALUE_SCHEMA).apply {
            put("id", id)
            put("client_id", clientId)
            put("scope", scope)
            put("name", name)
            put("value", valueJson)
            put("create_timestamp", System.currentTimeMillis())
            put("version", 1)
        }
        val out = ByteArrayOutputStream()
        out.write(WIRE_FORMAT_MAGIC_BYTE)
        out.write(ByteBuffer.allocate(4).putInt(FIXTURE_SCHEMA_ID).array())
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        GenericDatumWriter<GenericData.Record>(VALUE_SCHEMA).write(record, encoder)
        encoder.flush()
        return out.toByteArray()
    }

    companion object {
        private const val WIRE_FORMAT_MAGIC_BYTE = 0
        private const val FIXTURE_SCHEMA_ID = 1

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
    }
}

package org.radarbase.stream.ruleengine.serde

import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import org.radarbase.stream.ruleengine.domain.RuleKey

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

class RuleKeySerde : Serde<RuleKey> {
    private val avroSerde = AvroSerde(KEY_SCHEMA)

    override fun serializer(): Serializer<RuleKey> = Serializer { topic, _ -> avroSerde.unsupportedProducer(topic) }

    override fun deserializer(): Deserializer<RuleKey> = Deserializer { _, data ->
        data?.let {
            val record = avroSerde.decode(it)
            RuleKey(
                clientId = record["client_id"]?.toString().orEmpty(),
                scope = record["scope"]?.toString().orEmpty(),
                name = record["name"]?.toString().orEmpty(),
            )
        }
    }
}

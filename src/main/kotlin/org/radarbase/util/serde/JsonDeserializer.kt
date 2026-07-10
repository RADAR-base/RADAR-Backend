package org.radarbase.util.serde

import com.fasterxml.jackson.databind.ObjectReader
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.LoggerFactory

class JsonDeserializer<T>(deserializedClass: Class<T>) : Deserializer<T> {
    private val logger = LoggerFactory.getLogger(JsonDeserializer::class.java)
    private val reader: ObjectReader = RadarSerde.GENERIC_READER.forType(deserializedClass)

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        // no configuration
    }

    override fun deserialize(topic: String, data: ByteArray?): T? {
        if (data == null) return null
        return try {
            reader.readValue(data)
        } catch (e: Exception) {
            logger.error("Failed to deserialize value for topic {}", topic, e)
            null
        }
    }

    override fun close() {
        // no-op
    }
}

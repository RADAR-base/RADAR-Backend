package org.radarbase.kotlin.util.serde

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectWriter
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory

class JsonSerializer<T>(private val type: Class<T>? = null) : Serializer<T> {
    private val logger = LoggerFactory.getLogger(JsonSerializer::class.java)
    private val writer: ObjectWriter = if (type == null) {
        RadarSerde.GENERIC_WRITER
    } else {
        RadarSerde.GENERIC_WRITER.forType(type)
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        // no-op
    }

    override fun serialize(topic: String, data: T?): ByteArray? = try {
        if (data == null) null else writer.writeValueAsBytes(data)
    } catch (e: JsonProcessingException) {
        logger.error("Cannot serialize value {} in topic {}", data, topic, e)
        null
    }

    override fun close() {
        // no-op
    }
}

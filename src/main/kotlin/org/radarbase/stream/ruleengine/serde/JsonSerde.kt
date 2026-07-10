package org.radarbase.stream.ruleengine.serde

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class JsonSerde<T : Any>(private val clazz: Class<T>? = null, private val typeRef: TypeReference<T>? = null) : Serde<T> {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    override fun serializer(): Serializer<T> = Serializer { _, data ->
        data?.let { mapper.writeValueAsBytes(it) }
    }

    override fun deserializer(): Deserializer<T> = Deserializer { _, data ->
        data?.let {
            if (clazz != null) {
                mapper.readValue(it, clazz)
            } else if (typeRef != null) {
                mapper.readValue(it, typeRef)
            } else {
                throw IllegalArgumentException("Either clazz or typeRef must be provided")
            }
        }
    }
}

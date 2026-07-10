package org.radarbase.util.serde

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectReader
import com.fasterxml.jackson.databind.ObjectWriter
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes

class RadarSerde<T>(type: Class<T>) {
    companion object {
        private val MAPPER = ObjectMapper().apply {
            setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
            setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        }
        internal val GENERIC_WRITER: ObjectWriter = MAPPER.writer()
        internal val GENERIC_READER: ObjectReader = MAPPER.reader()
    }

    private val jsonSerializer = JsonSerializer<T>(type)
    private val jsonDeserializer = JsonDeserializer<T>(type)

    fun getSerde(): Serde<T> = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)
}

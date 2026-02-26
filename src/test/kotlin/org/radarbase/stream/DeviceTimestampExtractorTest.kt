/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.stream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith

class DeviceTimestampExtractorTest {
    private val timestampExtractor = DeviceTimestampExtractor()
    private val topic = "TESTTopic"

    @Test
    fun extract() {
        val userSchema =
            "{" + "\"namespace\": \"test.radar.backend\", \"type\": \"record\"," + "\"name\": \"TestTimeExtract\"," + "\"fields\": [{\"name\": \"timeReceived\", \"type\": \"double\"}]}"
        val record = buildIndexedRecord(userSchema)
        val timeValue = 40880.051388
        record.put("timeReceived", timeValue)
        val consumerRecord = ConsumerRecord<Any, Any>(topic, 3, 30L, null, record as Any)
        val extracted = timestampExtractor.extract(consumerRecord, -1L)
        assertEquals((1000.0 * timeValue).toLong(), extracted)
    }

    @Test
    fun extractWithNotDoubleTimeReceived() {
        val userSchema =
            "{" + "\"namespace\": \"test.radar.backend\", \"type\": \"record\"," + "\"name\": \"TestTimeExtract\"," + "\"fields\": [{\"name\": \"timeReceived\", \"type\": \"string\"}]}"
        val record = buildIndexedRecord(userSchema)
        record.put("timeReceived", "timeValue")
        val consumerRecord = ConsumerRecord<Any, Any>(topic, 3, 30L, null, record as Any)
        assertFailsWith<RuntimeException>("Impossible to extract timeReceived from") {
            timestampExtractor.extract(consumerRecord, -1L)
        }
    }

    private fun buildIndexedRecord(userSchema: String): GenericRecord {
        val parser = Schema.Parser()
        val schema = parser.parse(userSchema)
        return GenericData.Record(schema)
    }
}

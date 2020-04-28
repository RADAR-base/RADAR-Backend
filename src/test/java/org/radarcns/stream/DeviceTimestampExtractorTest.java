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

package org.radarcns.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by nivethika on 20-12-16.
 */
public class DeviceTimestampExtractorTest {

    private DeviceTimestampExtractor timestampExtractor;
    private String topic;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        this.timestampExtractor = new DeviceTimestampExtractor();

        this.topic = "TESTTopic";
    }

    @Test
    public void extract() {
        String userSchema = "{\"namespace\": \"test.radar.backend\", \"type\": \"record\", "
                +"\"name\": \"TestTimeExtract\","
                +"\"fields\": [{\"name\": \"time\", \"type\": \"double\"}]}";
        GenericRecord record = buildIndexedRecord(userSchema);
        double timeValue = 40880.051388;
        record.put("time", timeValue);
        ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<>(topic, 3, 30, null, record);
        long extracted = this.timestampExtractor.extract(consumerRecord, -1L);
        assertEquals((long) (1000d * timeValue), extracted);
    }

    @Test
    public void extractWithNotDoubleTimeReceived() {
        String userSchema = "{\"namespace\": \"test.radar.backend\", \"type\": \"record\", "
                +"\"name\": \"TestTimeExtract\","
                +"\"fields\": [{\"name\": \"time\", \"type\": \"string\"}]}";
        GenericRecord record = buildIndexedRecord(userSchema);
        record.put("time", "timeValue");
        ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<>(topic, 3, 30, null, record);

        exception.expect(RuntimeException.class);
        exception.expectMessage("Impossible to extract timeReceived from");
        long extracted = this.timestampExtractor.extract(consumerRecord, -1L);
        assertNull(extracted);

    }

    private static GenericRecord buildIndexedRecord(String userSchema) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        return new GenericData.Record(schema);
    }
}

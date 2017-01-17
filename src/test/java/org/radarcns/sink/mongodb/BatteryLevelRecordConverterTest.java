/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.sink.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by nivethika on 4-1-17.
 */
public class BatteryLevelRecordConverterTest {

    private BatteryLevelRecordConverter converter;

    @Before
    public void setUp() {
        this.converter = new BatteryLevelRecordConverter();
    }

    @Test
    public void supportedSchemaNames() {
        Collection<String> values = this.converter.supportedSchemaNames();
        assertEquals(values.size(),1,0);
        assertEquals("org.radarcns.key.MeasurementKey-org.radarcns.empatica.EmpaticaE4BatteryLevel",values.toArray()[0]);
    }

    @Test
    public void convert() {

        Schema keySchema = SchemaBuilder.struct().field("userId", Schema.STRING_SCHEMA).field("sourceId", Schema.STRING_SCHEMA).build();
        Struct keyStruct = new Struct(keySchema);
        keyStruct.put("userId", "user1");
        keyStruct.put("sourceId", "source1");

        Schema valueSchema = SchemaBuilder.struct().field("batteryLevel", Schema.FLOAT32_SCHEMA).field("timeReceived", Schema.FLOAT64_SCHEMA).build();
        Struct valueStruct  = new Struct(valueSchema);
        valueStruct.put("batteryLevel", 12.23f);
        valueStruct.put("timeReceived", 823.889d);

        SinkRecord record = new SinkRecord("mine", 0, keySchema,
                keyStruct, valueSchema, valueStruct, 0);
        Document document = this.converter.convert(record);

        assertNotNull(document);
        assertEquals(document.get("user"), "user1");
        assertEquals(document.get("source"), "source1");
        assertEquals(document.get("batteryLevel"), 12.23f);
        assertEquals(document.get("timeReceived"), 823.889d);
    }

}

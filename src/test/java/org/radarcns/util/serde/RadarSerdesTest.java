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

package org.radarcns.util.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;
/**
 * Created by nivethika on 21-12-16.
 */
public class RadarSerdesTest {
    private RadarSerdes radarSerdes;

    @Before
    public void setUp() {
        this.radarSerdes = RadarSerdes.getInstance();
    }

    @Test
    public void getDoubleCollector() {
        Serde<DoubleValueCollector> valueCollectorSerde = this.radarSerdes.getDoubleCollector();
        assertNotNull(valueCollectorSerde);
        assertNotNull(valueCollectorSerde.serializer());
        assertEquals(valueCollectorSerde.serializer().getClass(), JsonSerializer.class);
        assertNotNull(valueCollectorSerde.deserializer());
        assertEquals(valueCollectorSerde.deserializer().getClass(), JsonDeserializer.class);
    }

    @Test
    public void getDoubleArrayCollector() {
        Serde<DoubleArrayCollector> doubelArrayCollector = this.radarSerdes.getDoubelArrayCollector();
        assertNotNull(doubelArrayCollector);
        assertNotNull(doubelArrayCollector.serializer());
        assertEquals(doubelArrayCollector.serializer().getClass(), JsonSerializer.class);
        assertNotNull(doubelArrayCollector.deserializer());
        assertEquals(doubelArrayCollector.deserializer().getClass(), JsonDeserializer.class);
    }
}

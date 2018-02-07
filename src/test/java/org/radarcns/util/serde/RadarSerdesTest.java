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
import org.radarcns.stream.collector.AggregateListCollector;
import org.radarcns.stream.collector.NumericAggregateCollector;

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
        Serde<NumericAggregateCollector> valueCollectorSerde = this.radarSerdes.getNumericAggregateCollector();
        assertNotNull(valueCollectorSerde);
        assertNotNull(valueCollectorSerde.serializer());
        assertEquals(valueCollectorSerde.serializer().getClass(), JsonSerializer.class);
        assertNotNull(valueCollectorSerde.deserializer());
        assertEquals(valueCollectorSerde.deserializer().getClass(), JsonDeserializer.class);
    }

    @Test
    public void getDoubleArrayCollector() {
        Serde<AggregateListCollector> doubleArrayCollector = this.radarSerdes.getAggregateListCollector();
        assertNotNull(doubleArrayCollector);
        assertNotNull(doubleArrayCollector.serializer());
        assertEquals(doubleArrayCollector.serializer().getClass(), JsonSerializer.class);
        assertNotNull(doubleArrayCollector.deserializer());
        assertEquals(doubleArrayCollector.deserializer().getClass(), JsonDeserializer.class);
    }
}

package org.radarcns.util.serde;

import org.apache.kafka.common.serialization.Serde;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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

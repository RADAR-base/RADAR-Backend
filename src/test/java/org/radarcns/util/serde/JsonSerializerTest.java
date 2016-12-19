package org.radarcns.util.serde;

import junit.framework.TestCase;

import org.radarcns.key.MeasurementKey;

public class JsonSerializerTest extends TestCase {
    public void testSerialize() throws Exception {
        JsonSerializer<MeasurementKey> serializer = new JsonSerializer<>();
        MeasurementKey key = new MeasurementKey("user", "source");
        String result = new String(serializer.serialize("mytest", key));
        assertEquals("{\"userId\":\"user\",\"sourceId\":\"source\"}", result);
    }
}
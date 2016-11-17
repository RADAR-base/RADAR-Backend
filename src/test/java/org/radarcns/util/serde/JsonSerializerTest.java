package org.radarcns.util.serde;

import junit.framework.TestCase;

import org.radarcns.key.MeasurementKey;

public class JsonSerializerTest extends TestCase {
    public void testSerialize() throws Exception {
        JsonSerializer<MeasurementKey> serializer = new JsonSerializer<>();
        String result = new String(serializer.serialize("mytest", new MeasurementKey("user", "source")));
        assertEquals("{\"userId\":\"user\",\"sourceId\":\"source\"}", result);
    }
}
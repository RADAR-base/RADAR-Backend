package org.radarcns.util.serde;

import junit.framework.TestCase;

import org.radarcns.key.MeasurementKey;

import java.nio.charset.Charset;

public class JsonDeserializerTest extends TestCase {
    public void testSerialize() throws Exception {
        byte[] json = "{\"userId\":\"user\",\"sourceId\":\"source\"}"
                .getBytes(Charset.forName("UTF-8"));
        JsonDeserializer<MeasurementKey> serializer = new JsonDeserializer<>(MeasurementKey.class);
        MeasurementKey key = serializer.deserialize("mytest", json);
        assertEquals("user", key.getUserId());
        assertEquals("source", key.getSourceId());
    }
}
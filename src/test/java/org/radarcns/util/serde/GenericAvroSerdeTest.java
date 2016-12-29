package org.radarcns.util.serde;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by nivethika on 21-12-16.
 */
public class GenericAvroSerdeTest {

    private GenericAvroSerde genericAvroSerde;

    @Before
    public void setUp() {
        this.genericAvroSerde = new GenericAvroSerde();
    }

    @Test
    public void serializer() {
        assertEquals(this.genericAvroSerde.serializer().getClass(), GenericAvroSerializer.class);
    }

    @Test
    public void deserializer() {
        assertEquals(this.genericAvroSerde.deserializer().getClass(), GenericAvroDeserializer.class);
    }

    @Test
    public void configure() {
        Map<String,String> map = new HashMap<>();
        map.put("schema.registry.url", "testvalue");
        this.genericAvroSerde.configure(map, false);
    }
}

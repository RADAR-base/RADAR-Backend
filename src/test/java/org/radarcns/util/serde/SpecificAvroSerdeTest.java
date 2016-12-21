package org.radarcns.util.serde;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by nivethika on 21-12-16.
 */
public class SpecificAvroSerdeTest {

    private SpecificAvroSerde specificAvroSerde ;

    @Before
    public void setUp() {
        this.specificAvroSerde = new SpecificAvroSerde();
    }

    @Test
    public void serializer() {
        assertEquals(this.specificAvroSerde.serializer().getClass(), SpecificAvroSerializer.class );
    }

    @Test
    public void deserializer() {
        assertEquals(this.specificAvroSerde.deserializer().getClass(), SpecificAvroDeserializer.class );
    }



}

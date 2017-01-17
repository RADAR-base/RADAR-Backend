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

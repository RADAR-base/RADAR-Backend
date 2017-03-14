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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

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

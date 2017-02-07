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

package org.radarcns.empatica.topic;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by nivethika on 21-12-16.
 */
public class E4StreamsTest {
    private E4Streams e4Streams;

    @Before
    public void setUp() {
        this.e4Streams = E4Streams.getInstance();
    }

    @Test
    public void getSensorTopics() {
        assertEquals(this.e4Streams.getSensorStreams(), E4SensorStreams.getInstance());
    }

    @Test
    public void getInternalTopics() {
        assertEquals(this.e4Streams.getInternalStreams(), E4InternalStreams.getInstance());
    }

    @Test
    public void getTopicNames() {
        assertEquals(15, this.e4Streams.getTopicNames().size()); // sort removes the redundant
    }
}

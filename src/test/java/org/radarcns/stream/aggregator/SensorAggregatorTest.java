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

package org.radarcns.stream.aggregator;

import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.topic.KafkaTopic;

import java.io.IOException;
import org.radarcns.topic.StreamDefinition;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
/**
 * Created by nivethika on 20-12-16.
 */
public class SensorAggregatorTest {
    private AggregatorWorker aggregator;
    @Before
    public void setUp() {
        aggregator = mock(AggregatorWorker.class);
    }

    @Test
    public void getBuilder() throws IOException {
        String topicName = "TESTTopic";
        StreamDefinition sensorTopic = new StreamDefinition(new KafkaTopic(topicName), new KafkaTopic(topicName + "_output"));
        when(aggregator.getStreamDefinition()).thenReturn(sensorTopic);
        doCallRealMethod().when(aggregator).getBuilder();
        KStreamBuilder builder = aggregator.getBuilder();

        verify(aggregator, times(1)).setStream(any());
    }

}

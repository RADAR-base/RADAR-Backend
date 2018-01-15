/*
 * Copyright 2017 King's College London and The Hyve
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

package org.radarcns.stream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.config.KafkaProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.topic.KafkaTopic;
import org.radarcns.util.RadarSingletonFactory;

/**
 * Created by nivethika on 20-12-16.
 */
public class KStreamWorkerTest {
    private KStreamWorker aggregator;
    @Before
    public void setUp() {
        aggregator = mock(KStreamWorker.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getBuilder() throws IOException {
        String topicName = "TESTTopic";
        StreamDefinition sensorTopic = new StreamDefinition(new KafkaTopic(topicName), new KafkaTopic(topicName + "_output"));
        when(aggregator.getStreamDefinitions()).thenReturn(Collections.singleton(sensorTopic));
        KStreamBuilder builder = new KStreamBuilder();
        when(aggregator.getBuilder()).thenReturn(builder);

        RadarPropertyHandler propertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        propertyHandler.load("src/test/resources/config/radar.yml");
        KafkaProperty kafkaProperty = propertyHandler.getKafkaProperties();
        when(aggregator.getStreamProperties(eq(sensorTopic))).thenReturn(
                kafkaProperty.getStreamProperties("test", 1, DeviceTimestampExtractor.class));
        when(aggregator.implementStream(eq(sensorTopic), any())).thenReturn(mock(KStream.class));
        doCallRealMethod().when(aggregator).createBuilder(sensorTopic);
        aggregator.createBuilder(sensorTopic);

        verify(aggregator, times(1)).implementStream(eq(sensorTopic), any());
    }
}

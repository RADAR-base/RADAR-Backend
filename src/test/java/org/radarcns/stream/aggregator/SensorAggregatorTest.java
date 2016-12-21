package org.radarcns.stream.aggregator;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.AvroTopic;
import org.radarcns.topic.SensorTopic;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by nivethika on 20-12-16.
 */
public class SensorAggregatorTest {
    private SensorAggregator aggregator;
    @Before
    public void setUp() {
        aggregator = mock(SensorAggregator.class);
    }

    @Test
    public void getBuilder() throws IOException {
        String topicName = "TESTTopic";
        SensorTopic sensorTopic = new SensorTopic(topicName, String.class);
        when(aggregator.getTopic()).thenReturn(sensorTopic);
        doCallRealMethod().when(aggregator).getBuilder();
        KStreamBuilder builder =aggregator.getBuilder();

        verify(aggregator, times(1)).setStream(any(), eq(sensorTopic));
    }

}

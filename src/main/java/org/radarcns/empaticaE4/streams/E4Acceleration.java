package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.empaticaE4.EmpaticaE4Acceleration;
import org.radarcns.empaticaE4.topic.E4Topics;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.stream.aggregator.SensorAggregator;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.topic.sensor.SensorTopic;
import org.radarcns.util.RadarUtils;
import org.radarcns.util.serde.RadarSerdes;

import java.io.IOException;

/**
 * Definition of Kafka Stream for aggregating data collected by Empatica E4 Acceleromete
 */
public class E4Acceleration extends SensorAggregator<EmpaticaE4Acceleration> {

    public E4Acceleration(String clientID, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getAccelerationTopic(),clientID,master);
    }

    public E4Acceleration(String clientID, int numThread, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getAccelerationTopic(),clientID,numThread,master);
    }


    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4Acceleration> kstream, SensorTopic<EmpaticaE4Acceleration> topic) throws IOException {
        kstream.groupByKey().aggregate(
                () -> new DoubleArrayCollector(3),
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.accelerationToArray(v)),
                TimeWindows.of(10 * 1000L),
                RadarSerdes.getInstance().getDoubelArrayCollector(),
                topic.getStateStoreName())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

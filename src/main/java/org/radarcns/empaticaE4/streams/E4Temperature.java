package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.empaticaE4.EmpaticaE4Temperature;
import org.radarcns.empaticaE4.topic.E4Topics;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.stream.aggregator.SensorAggregator;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.topic.sensor.SensorTopic;
import org.radarcns.util.RadarUtils;
import org.radarcns.util.serde.RadarSerdes;

import java.io.IOException;

/**
 * Definition of Kafka Stream for aggregating temperature values collected by Empatica E4
 */
public class E4Temperature extends SensorAggregator<EmpaticaE4Temperature> {

    public E4Temperature(String clientID, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getTemperatureTopic(),clientID,master);
    }

    public E4Temperature(String clientID,int numThread, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getTemperatureTopic(),clientID,numThread,master);
    }


    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4Temperature> kstream, SensorTopic<EmpaticaE4Temperature> topic) throws IOException {
        kstream.aggregateByKey(DoubleValueCollector::new,
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.floatToDouble(v.getTemperature())),
                TimeWindows.of(topic.getInProgessTopic(), 10000),
                topic.getKeySerde(), RadarSerdes.getInstance().getDoubelCollector())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

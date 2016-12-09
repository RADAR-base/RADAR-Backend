package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.empaticaE4.EmpaticaE4BatteryLevel;
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
 * Definition of Kafka Stream for aggregating data about Empatica E4 battery level
 */
public class E4BatteryLevel extends SensorAggregator<EmpaticaE4BatteryLevel> {

    public E4BatteryLevel(String clientID, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getBatteryLevelTopic(),clientID,master);
    }

    public E4BatteryLevel(String clientID, int numThread, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getBatteryLevelTopic(),clientID,numThread,master);
    }


    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4BatteryLevel> kstream, SensorTopic<EmpaticaE4BatteryLevel> topic) throws IOException {
        kstream.aggregateByKey(DoubleValueCollector::new,
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.floatToDouble(v.getBatteryLevel())),
                TimeWindows.of(topic.getInProgessTopic(), 10000),
                topic.getKeySerde(), RadarSerdes.getInstance().getDoubelCollector())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

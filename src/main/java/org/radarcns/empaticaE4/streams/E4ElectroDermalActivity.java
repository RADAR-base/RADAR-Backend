package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.empaticaE4.EmpaticaE4ElectroDermalActivity;
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
 * Definition of Kafka Stream for aggregating data about Blood Volume Pulse collected by Empatica E4
 */
public class E4ElectroDermalActivity extends SensorAggregator<EmpaticaE4ElectroDermalActivity> {

    public E4ElectroDermalActivity(String clientID, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getElectroDermalActivityTopic(),clientID,master);
    }

    public E4ElectroDermalActivity(String clientID, int numThread, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getElectroDermalActivityTopic(),clientID,numThread,master);
    }

    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4ElectroDermalActivity> kstream, SensorTopic<EmpaticaE4ElectroDermalActivity> topic) throws IOException {
        kstream.aggregateByKey(DoubleValueCollector::new,
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.floatToDouble(v.getElectroDermalActivity())),
                TimeWindows.of(topic.getInProgessTopic(), 10000),
                topic.getKeySerde(), RadarSerdes.getInstance().getDoubelCollector())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

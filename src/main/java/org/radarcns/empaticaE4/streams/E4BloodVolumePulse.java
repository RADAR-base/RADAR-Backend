package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.empaticaE4.EmpaticaE4BloodVolumePulse;
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
 * Created by Francesco Nobilia on 11/10/2016.
 */
public class E4BloodVolumePulse extends SensorAggregator<EmpaticaE4BloodVolumePulse> {

    public E4BloodVolumePulse(String clientID, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getBloodVolumePulseTopic(), clientID, master);
    }

    public E4BloodVolumePulse(String clientID, int numThread, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getBloodVolumePulseTopic(), clientID, numThread, master);
    }

    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4BloodVolumePulse> kstream, SensorTopic<EmpaticaE4BloodVolumePulse> topic) throws IOException {
        kstream.groupByKey().aggregate(
                DoubleValueCollector::new,
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.floatToDouble(v.getBloodVolumePulse())),
                TimeWindows.of(10 * 1000L),
                RadarSerdes.getInstance().getDoubelCollector(),
                topic.getStateStoreName())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

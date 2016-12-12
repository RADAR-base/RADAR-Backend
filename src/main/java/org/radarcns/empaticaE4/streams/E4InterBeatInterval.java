package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
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
 * Definition of Kafka Stream for aggregating Inter Beat Interval values collected by Empatica E4
 */
public class E4InterBeatInterval extends SensorAggregator<EmpaticaE4InterBeatInterval> {

    public E4InterBeatInterval(String clientID, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getInterBeatIntervalTopic(), clientID,master);
    }

    public E4InterBeatInterval(String clientID, int numThread, MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getInterBeatIntervalTopic(), clientID, numThread,master);
    }

    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4InterBeatInterval> kstream, SensorTopic<EmpaticaE4InterBeatInterval> topic) throws IOException {
        kstream.groupByKey().aggregate(
                DoubleValueCollector::new,
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.floatToDouble(v.getInterBeatInterval())),
                TimeWindows.of(10 * 1000L),
                RadarSerdes.getInstance().getDoubelCollector(),
                topic.getStateStoreName())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

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
import org.radarcns.topic.SensorTopic;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.RadarUtils;
import org.radarcns.util.serde.RadarSerdes;

import javax.annotation.Nonnull;
import java.io.IOException;

public class E4BloodVolumePulse extends SensorAggregator<EmpaticaE4BloodVolumePulse> {
    private RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();
    public E4BloodVolumePulse(String clientID, int numThread,
                              MasterAggregator master) throws IOException{
        super(E4Topics.getInstance().getSensorTopics().getBloodVolumePulseTopic(),
                clientID, numThread, master);
    }

    @Override
    protected void setStream(@Nonnull KStream<MeasurementKey, EmpaticaE4BloodVolumePulse> kstream,
                             @Nonnull SensorTopic<EmpaticaE4BloodVolumePulse> topic)
            throws IOException {
        kstream.groupByKey()
                .aggregate(
                    DoubleValueCollector::new,
                    (k, v, valueCollector) -> valueCollector.add(v.getBloodVolumePulse()),
                    TimeWindows.of(10 * 1000L),
                    RadarSerdes.getInstance().getDoubleCollector(),
                    topic.getStateStoreName())
                .toStream()
                .map((k, v) -> new KeyValue<>(utilities.getWindowed(k), v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

package org.radarcns.empatica.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.config.KafkaProperty;
import org.radarcns.empatica.EmpaticaE4InterBeatInterval;
import org.radarcns.empatica.topic.E4Topics;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.aggregator.InternalAggregator;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.topic.InternalTopic;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Kafka Stream for computing and aggregating Heart Rate values collected by Empatica E4
 */
public class E4HeartRate extends
        InternalAggregator<EmpaticaE4InterBeatInterval, DoubleAggregator> {
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public E4HeartRate(String clientId, int numThread, MasterAggregator master,
            KafkaProperty kafkaProperties) throws IOException {
        super(E4Topics.getInstance().getInternalTopics().getHeartRateTopic(), clientId, numThread,
                master, kafkaProperties);
    }

    @Override
    protected void setStream(@Nonnull KStream<MeasurementKey, EmpaticaE4InterBeatInterval> kstream,
            @Nonnull InternalTopic<DoubleAggregator> topic) throws IOException {
        kstream.groupByKey()
                .aggregate(
                    DoubleValueCollector::new,
                    (k, v, valueCollector) -> valueCollector.add(
                            utilities.ibiToHeartRate(v.getInterBeatInterval())),
                    TimeWindows.of(10 * 1000L),
                    RadarSerdes.getInstance().getDoubleCollector(),
                    topic.getStateStoreName())
                .toStream()
                .map((k,v) -> new KeyValue<>(utilities.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }

}

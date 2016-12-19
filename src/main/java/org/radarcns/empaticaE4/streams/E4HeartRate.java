package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
import org.radarcns.empaticaE4.topic.E4Topics;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.aggregator.InternalAggregator;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.topic.InternalTopic;
import org.radarcns.util.RadarUtils;
import org.radarcns.util.serde.RadarSerdes;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Definition of Kafka Stream for computing and aggregating Heart Rate values collected by Empatica E4
 */
public class E4HeartRate extends InternalAggregator<EmpaticaE4InterBeatInterval, DoubleAggregator> {

    public E4HeartRate(String clientID, int numThread, MasterAggregator master) throws IOException {
        super(E4Topics.getInstance().getInternalTopics().getHeartRateTopic(), clientID, numThread,
                master);
    }

    @Override
    protected void setStream(@Nonnull KStream<MeasurementKey, EmpaticaE4InterBeatInterval> kstream,
                             @Nonnull InternalTopic<DoubleAggregator> topic) throws IOException {
        kstream.groupByKey()
                .aggregate(
                    DoubleValueCollector::new,
                    (k, v, valueCollector) -> valueCollector.add(
                            RadarUtils.ibiToHR(v.getInterBeatInterval())),
                    TimeWindows.of(10 * 1000L),
                    RadarSerdes.getInstance().getDoubleCollector(),
                    topic.getStateStoreName())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }

}

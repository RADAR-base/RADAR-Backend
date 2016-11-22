package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.aggregator.DoubleAggegator;
import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
import org.radarcns.empaticaE4.topic.E4Topics;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.aggregator.InternalAggregator;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.topic.Internal.InternalTopic;
import org.radarcns.util.RadarUtils;

import java.io.IOException;

/**
 * Created by Francesco Nobilia on 11/10/2016.
 * This Internal consumes the Empatica E4 topic for E4InterBeatInterval sensor and
 * trasform it in a Heart Rate value
 */
public class E4HeartRate extends InternalAggregator<EmpaticaE4InterBeatInterval,DoubleAggegator> {

    public E4HeartRate(String clientID) throws IOException {
        super(E4Topics.getInstance().getInternalTopics().getHeartRateTopic(),clientID);
    }

    public E4HeartRate(String clientID, int numThread) throws IOException {
        super(E4Topics.getInstance().getInternalTopics().getHeartRateTopic(),clientID,numThread);
    }

    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4InterBeatInterval> kstream, InternalTopic<DoubleAggegator> topic) throws IOException {

        kstream.aggregateByKey(DoubleValueCollector::new,
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.ibiToHR(v.getInterBeatInterval())),
                TimeWindows.of(topic.getInProgessTopic(), 10000),
                topic.getMeasurementKeySerde(),topic.getDoubleCollectorSerde())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }
}

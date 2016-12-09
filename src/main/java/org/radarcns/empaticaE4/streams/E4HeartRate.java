package org.radarcns.empaticaE4.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.aggregator.DoubleAggegator;
import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
import org.radarcns.empaticaE4.topic.E4Topics;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.aggregator.InternalAggregator;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.topic.internal.InternalTopic;
import org.radarcns.util.RadarUtils;
import org.radarcns.util.serde.RadarSerdes;

import java.io.IOException;

/**
 * Definition of Kafka Stream for computing and aggregating Heart Rate values collected by Empatica E4
 */
public class E4HeartRate extends InternalAggregator<EmpaticaE4InterBeatInterval,DoubleAggegator> {

    public E4HeartRate(String clientID, MasterAggregator master) throws IOException {
        super(E4Topics.getInstance().getInternalTopics().getHeartRateTopic(),clientID,master);
    }

    public E4HeartRate(String clientID, int numThread, MasterAggregator master) throws IOException {
        super(E4Topics.getInstance().getInternalTopics().getHeartRateTopic(),clientID,numThread,master);
    }

    @Override
    protected void setStream(KStream<MeasurementKey, EmpaticaE4InterBeatInterval> kstream, InternalTopic<DoubleAggegator> topic) throws IOException {

        kstream.aggregateByKey(DoubleValueCollector::new,
                (k, v, valueCollector) -> valueCollector.add(RadarUtils.ibiToHR(v.getInterBeatInterval())),
                TimeWindows.of(topic.getInProgessTopic(), 10000),
                RadarSerdes.getInstance().getMeasurementKeySerde(),RadarSerdes.getInstance().getDoubelCollector())
                .toStream()
                .map((k,v) -> new KeyValue<>(RadarUtils.getWindowed(k),v.convertInAvro()))
                .to(topic.getOutputTopic());
    }

}

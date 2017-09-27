package org.radarcns.stream.phone;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.config.KafkaProperty;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.passive.phone.PhoneBatteryLevel;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.aggregator.DoubleAggregation;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class PhoneBatteryStream extends StreamWorker<ObservationKey, PhoneBatteryLevel> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneBatteryStream.class);
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public PhoneBatteryStream(String phoneBatteryStream, int priority, StreamMaster master,
            KafkaProperty kafkaProperty) {
        super(PhoneStreams.getInstance().getBatteryStream(), phoneBatteryStream,
                priority, master,
                kafkaProperty, logger);
    }

    @Override
    protected KStream<AggregateKey, DoubleAggregation> defineStream(
            @Nonnull KStream<ObservationKey, PhoneBatteryLevel> kstream) {
        return kstream.groupByKey()
                .aggregate(
                        DoubleValueCollector::new,
                        (k, v, valueCollector) -> valueCollector.add(v.getBatteryLevel()),
                        TimeWindows.of(10 * 1000L),
                        RadarSerdes.getInstance().getDoubleCollector(),
                        getStreamDefinition().getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro);
    }
}

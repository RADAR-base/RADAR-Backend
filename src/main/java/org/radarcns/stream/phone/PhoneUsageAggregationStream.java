package org.radarcns.stream.phone;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.phone.PhoneUsageEvent;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.stream.empatica.E4BatteryLevelStream;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;


/**
 * Created by piotrzakrzewski on 26/07/2017.
 */
public class PhoneUsageAggregationStream extends StreamWorker<MeasurementKey, PhoneUsageEvent> {

    private static final Logger log = LoggerFactory.getLogger(PhoneUsageAggregationStream.class);
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public PhoneUsageAggregationStream(@Nonnull StreamDefinition streamDefinition, @Nonnull String clientId, int numThreads, @Nonnull StreamMaster aggregator, KafkaProperty kafkaProperty, Logger monitorLog) {
        super(streamDefinition, clientId, numThreads, aggregator, kafkaProperty, monitorLog);
    }

    @Override
    protected KStream<?, ?> defineStream(@Nonnull KStream<MeasurementKey, PhoneUsageEvent> kstream) {
        return kstream.groupByKey()
                .aggregate(
                        PhoneUsageCollector::new,
                        (k, v, valueCollector) -> valueCollector.update(v.getEventType(), v.getPackageName(), v.getTime()),
                        TimeWindows.of(10 * 1000L),
                        RadarSerdes.getInstance().getDoubleCollector(),
                        getStreamDefinition().getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro);
    }
}

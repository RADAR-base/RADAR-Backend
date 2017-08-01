package org.radarcns.stream.phone;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.phone.PhoneUsageEvent;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.stream.empatica.E4BatteryLevelStream;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.Map;


/**
 * Created by piotrzakrzewski on 26/07/2017.
 */
public class PhoneUsageAggregationStream extends StreamWorker<MeasurementKey, PhoneUsageEvent> {

    private static final Logger log = LoggerFactory.getLogger(PhoneUsageAggregationStream.class);
    private static final long dayInMs = 24 * 60 * 60 * 1000;
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public PhoneUsageAggregationStream(@Nonnull StreamDefinition streamDefinition, @Nonnull String clientId, int numThreads, @Nonnull StreamMaster aggregator, KafkaProperty kafkaProperty, Logger monitorLog) {
        super(streamDefinition, clientId, numThreads, aggregator, kafkaProperty, monitorLog);
    }

    @Override
    protected KStream<?, ?> defineStream(@Nonnull KStream<MeasurementKey, PhoneUsageEvent> kstream) {
        return kstream.groupBy((k, v) -> getTuplekey(k, v.getPackageName()) )
                .aggregate(
                        PhoneUsageCollector::new,
                        (k, phoneUsageEvent, valueCollector) -> valueCollector.update(phoneUsageEvent),
                        TimeWindows.of(dayInMs),
                        RadarSerdes.getInstance().getPhoneUsageCollector(),
                        getStreamDefinition().getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro);
    }

    private Map.Entry<MeasurementKey, String> getTuplekey(MeasurementKey key, String packageName) {
        return new java.util.AbstractMap.SimpleEntry<>(key, packageName);
    }


}

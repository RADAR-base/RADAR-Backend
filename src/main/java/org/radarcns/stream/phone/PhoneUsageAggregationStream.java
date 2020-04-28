package org.radarcns.stream.phone;

import java.time.Duration;
import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneUsageEvent;
import org.radarcns.stream.SensorStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.aggregator.PhoneUsageAggregate;
import org.radarcns.util.serde.RadarSerdes;

/**
 * Created by piotrzakrzewski on 26/07/2017.
 */
public class PhoneUsageAggregationStream extends
        SensorStreamWorker<ObservationKey, PhoneUsageEvent> {
    @Override
    protected void initialize() {
        defineStream(
                "android_phone_usage_event_output",
                "android_phone_usage_event_aggregated",
                Duration.ofDays(1));
        config.setDefaultPriority(Priority.LOW);
    }

    @Override
    protected KStream<AggregateKey, PhoneUsageAggregate> implementStream(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneUsageEvent> kstream) {
        return kstream.groupBy(PhoneUsageAggregationStream::temporaryKey)
                .windowedBy(definition.getTimeWindows())
                .aggregate(
                        PhoneUsageCollector::new,
                        (k, v, valueCollector) -> valueCollector.update(v),
                        RadarSerdes.materialized(definition.getStateStoreName(),
                                RadarSerdes.getInstance(allConfig.getSchemaRegistryPaths())
                                        .getPhoneUsageCollector(
                                                getStreamPropertiesMap(definition), false)))
                .toStream()
                .map(utilities::phoneCollectorToAvro);
    }

    private static TemporaryPackageKey temporaryKey(ObservationKey key, PhoneUsageEvent value) {
        return new TemporaryPackageKey(key.getProjectId(), key.getUserId(), key.getSourceId(),
                value.getPackageName());
    }
}

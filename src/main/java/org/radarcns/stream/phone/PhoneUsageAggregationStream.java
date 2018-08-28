package org.radarcns.stream.phone;

import java.time.Duration;
import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneUsageEvent;
import org.radarcns.stream.KStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.aggregator.PhoneUsageAggregate;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by piotrzakrzewski on 26/07/2017.
 */
public class PhoneUsageAggregationStream extends KStreamWorker<ObservationKey, PhoneUsageEvent> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneUsageAggregationStream.class);

    public PhoneUsageAggregationStream(int numThread, StreamMaster master,
            RadarPropertyHandler properties) {
        super(numThread, master, properties, logger);
        createStream(
            "android_phone_usage_event_output",
            "android_phone_usage_event_aggregated",
            Duration.ofDays(1));
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
                                RadarSerdes.getInstance().getPhoneUsageCollector()))
                .toStream()
                .map(utilities::phoneCollectorToAvro);
    }

    private static TemporaryPackageKey temporaryKey(ObservationKey key, PhoneUsageEvent value) {
        return new TemporaryPackageKey(key.getProjectId(), key.getUserId(), key.getSourceId(),
                value.getPackageName());
    }
}

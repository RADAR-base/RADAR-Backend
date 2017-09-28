package org.radarcns.stream.phone;

import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.aggregator.PhoneUsageAggregator;
import org.radarcns.config.KafkaProperty;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneUsageEvent;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Created by piotrzakrzewski on 26/07/2017.
 */
public class PhoneUsageAggregationStream extends StreamWorker<ObservationKey, PhoneUsageEvent> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneUsageAggregationStream.class);

    public PhoneUsageAggregationStream(Collection<StreamDefinition> definitions, int numThread,
            StreamMaster master, KafkaProperty kafkaProperties) {
        super(definitions, numThread, master, kafkaProperties, logger);
    }

    @Override
    protected KStream<AggregateKey, PhoneUsageAggregator> implementStream(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneUsageEvent> kstream) {
        return kstream.groupBy((k, v) -> new TemporaryPackageKey(k, v.getPackageName()))
                .aggregate(
                        PhoneUsageCollector::new,
                        (k, v, valueCollector) -> valueCollector.update(v),
                        definition.getTimeWindows(),
                        RadarSerdes.getInstance().getPhoneUsageCollector(),
                        definition.getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro);
    }
}

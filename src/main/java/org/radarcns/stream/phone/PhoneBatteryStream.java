package org.radarcns.stream.phone;

import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.KafkaProperty;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneBatteryLevel;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.aggregator.DoubleAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;

public class PhoneBatteryStream extends StreamWorker<ObservationKey, PhoneBatteryLevel> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneBatteryStream.class);

    public PhoneBatteryStream(Collection<StreamDefinition> definitions, int numThread,
            StreamMaster master, KafkaProperty kafkaProperties) {
        super(definitions, numThread, master, kafkaProperties, logger);
    }

    @Override
    protected KStream<AggregateKey, DoubleAggregation> implementStream(StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneBatteryLevel> kstream) {
        return aggregateFloat(definition, kstream, PhoneBatteryLevel::getBatteryLevel);
    }
}

package org.radarcns.stream.phone;

import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.KafkaProperty;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneAcceleration;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.aggregator.DoubleArrayAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;

import static org.radarcns.util.Serialization.floatToDouble;

public class PhoneAccelerationStream extends StreamWorker<ObservationKey, PhoneAcceleration> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneAccelerationStream.class);

    public PhoneAccelerationStream(Collection<StreamDefinition> definitions, int numThread,
            StreamMaster master, KafkaProperty kafkaProperties) {
        super(definitions, numThread, master, kafkaProperties, logger);
    }

    @Override
    protected KStream<AggregateKey, DoubleArrayAggregation> implementStream(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneAcceleration> kstream) {
        return aggregateDoubleArray(definition, kstream, v -> new double[] {
            floatToDouble(v.getX()),
            floatToDouble(v.getY()),
            floatToDouble(v.getZ())
        });
    }
}

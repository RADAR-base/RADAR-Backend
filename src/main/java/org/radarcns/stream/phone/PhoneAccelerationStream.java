package org.radarcns.stream.phone;

import java.util.Collection;
import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneAcceleration;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.aggregator.AggregateList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneAccelerationStream extends StreamWorker<ObservationKey, PhoneAcceleration> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneAccelerationStream.class);

    public PhoneAccelerationStream(Collection<StreamDefinition> definitions, int numThread,
            StreamMaster master, RadarPropertyHandler properties) {
        super(definitions, numThread, master, properties, logger);
    }

    @Override
    protected KStream<AggregateKey, AggregateList> implementStream(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneAcceleration> kstream) {
        return aggregateFields(definition, kstream, new String[] {"x", "y", "z"},
                PhoneAcceleration.getClassSchema());
    }
}

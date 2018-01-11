package org.radarcns.stream.phone;

import java.util.Collection;
import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneBatteryLevel;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.KStreamWorker;
import org.radarcns.stream.aggregator.NumericAggregate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneBatteryStream extends KStreamWorker<ObservationKey, PhoneBatteryLevel> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneBatteryStream.class);

    public PhoneBatteryStream(Collection<StreamDefinition> definitions, int numThread,
            StreamMaster master, RadarPropertyHandler properties) {
        super(definitions, numThread, master, properties, logger);
    }

    @Override
    protected KStream<AggregateKey, NumericAggregate> implementStream(StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneBatteryLevel> kstream) {
        return aggregateNumeric(definition, kstream, "batteryLevel",
                PhoneBatteryLevel.getClassSchema());
    }
}

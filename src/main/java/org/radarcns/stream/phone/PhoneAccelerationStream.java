package org.radarcns.stream.phone;

import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneAcceleration;
import org.radarcns.stream.SensorStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.aggregator.AggregateList;

public class PhoneAccelerationStream extends SensorStreamWorker<ObservationKey, PhoneAcceleration> {
    @Override
    protected void initialize() {
        defineWindowedSensorStream("android_phone_acceleration");
        config.setDefaultPriority(Priority.HIGH);
    }

    @Override
    protected KStream<AggregateKey, AggregateList> implementStream(
            StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneAcceleration> kstream) {
        return aggregateFields(definition, kstream, new String[] {"x", "y", "z"},
                PhoneAcceleration.getClassSchema());
    }
}

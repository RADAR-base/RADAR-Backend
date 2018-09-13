package org.radarcns.stream.phone;

import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneBatteryLevel;
import org.radarcns.stream.SensorStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.aggregator.NumericAggregate;

public class PhoneBatteryStream extends SensorStreamWorker<ObservationKey, PhoneBatteryLevel> {
    @Override
    protected void initialize() {
        defineWindowedSensorStream("android_phone_battery_level");
        config.setDefaultPriority(Priority.LOW);
    }

    @Override
    protected KStream<AggregateKey, NumericAggregate> implementStream(StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneBatteryLevel> kstream) {
        return aggregateNumeric(definition, kstream, "batteryLevel",
                PhoneBatteryLevel.getClassSchema());
    }
}

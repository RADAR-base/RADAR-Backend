package org.radarbase.stream.phone;

import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.radarbase.config.RadarPropertyHandler.Priority;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneBatteryLevel;
import org.radarbase.stream.SensorStreamWorker;
import org.radarbase.stream.StreamDefinition;
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

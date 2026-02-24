package org.radarbase.kotlin.stream.phone

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.kotlin.config.RadarPropertyHandler
import org.radarbase.kotlin.stream.SensorStreamWorker
import org.radarbase.kotlin.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneBatteryLevel
import org.radarcns.stream.aggregator.NumericAggregate

class PhoneBatteryStream : SensorStreamWorker<ObservationKey, PhoneBatteryLevel>() {
    override fun initialize() {
        defineWindowedSensorStream("android_phone_battery_level")
        config.setDefaultPriority(RadarPropertyHandler.Priority.LOW)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, PhoneBatteryLevel>
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateNumeric(
            definition, kstream, "batteryLevel",
            PhoneBatteryLevel.getClassSchema()
        )
    }
}

package org.radarbase.stream.phone

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarConfigHandler
import org.radarbase.stream.SensorStreamWorker
import org.radarbase.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneAcceleration
import org.radarcns.stream.aggregator.AggregateList

class PhoneAccelerationStream : SensorStreamWorker<ObservationKey, PhoneAcceleration>() {
    override fun initialize() {
        defineWindowedSensorStream("android_phone_acceleration")
        config.setDefaultPriority(RadarConfigHandler.Priority.HIGH)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, PhoneAcceleration>,
    ): KStream<AggregateKey, AggregateList> {
        return aggregateFields(
            definition,
            kstream,
            arrayOf("x", "y", "z"),
            PhoneAcceleration.getClassSchema(),
        )
    }
}

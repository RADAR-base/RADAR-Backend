package org.radarbase.kotlin.stream.phone

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.kotlin.stream.SensorStreamWorker
import org.radarbase.kotlin.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneAcceleration
import org.radarcns.stream.aggregator.AggregateList

class PhoneAccelerationStream : SensorStreamWorker<ObservationKey, PhoneAcceleration>() {
    override fun initialize() {
        defineWindowedSensorStream("android_phone_acceleration")
        config.setDefaultPriority(RadarPropertyHandler.Priority.HIGH)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, PhoneAcceleration>
    ): KStream<AggregateKey, AggregateList> {
        return aggregateFields(
            definition, kstream, arrayOf("x", "y", "z"),
            PhoneAcceleration.getClassSchema()
        )
    }
}

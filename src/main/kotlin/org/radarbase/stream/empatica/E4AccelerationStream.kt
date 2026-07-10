package org.radarbase.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarConfigHandler
import org.radarbase.stream.SensorStreamWorker
import org.radarbase.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4Acceleration
import org.radarcns.stream.aggregator.AggregateList

/**
 * Definition of Kafka Stream for aggregating data collected by Empatica E4 Accelerometer sensor.
 */
class E4AccelerationStream : SensorStreamWorker<ObservationKey, EmpaticaE4Acceleration>() {
    override fun initialize() {
        defineWindowedSensorStream("android_empatica_e4_acceleration")
        config.setDefaultPriority(RadarConfigHandler.Priority.HIGH)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4Acceleration>,
    ): KStream<AggregateKey, AggregateList> {
        return aggregateFields(
            definition,
            kstream,
            arrayOf("x", "y", "z"),
            EmpaticaE4Acceleration.getClassSchema(),
        )
    }
}

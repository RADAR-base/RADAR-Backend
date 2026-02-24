package org.radarbase.kotlin.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.kotlin.config.RadarPropertyHandler
import org.radarbase.kotlin.stream.SensorStreamWorker
import org.radarbase.kotlin.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse
import org.radarcns.stream.aggregator.NumericAggregate

/**
 * Kafka Stream for aggregating data about Blood Volume Pulse collected by Empatica E4.
 */
class E4BloodVolumePulseStream : SensorStreamWorker<ObservationKey, EmpaticaE4BloodVolumePulse>() {
    override fun initialize() {
        defineWindowedSensorStream("android_empatica_e4_blood_volume_pulse")
        config.setDefaultPriority(RadarPropertyHandler.Priority.HIGH)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4BloodVolumePulse>
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateNumeric(
            definition, kstream, "bloodVolumePulse",
            EmpaticaE4BloodVolumePulse.getClassSchema()
        )
    }
}

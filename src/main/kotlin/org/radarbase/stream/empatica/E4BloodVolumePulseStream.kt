package org.radarbase.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarConfigHandler
import org.radarbase.stream.SensorStreamWorker
import org.radarbase.stream.StreamDefinition
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
        config.setDefaultPriority(RadarConfigHandler.Priority.HIGH)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4BloodVolumePulse>,
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateNumeric(
            definition,
            kstream,
            "bloodVolumePulse",
            EmpaticaE4BloodVolumePulse.getClassSchema(),
        )
    }
}

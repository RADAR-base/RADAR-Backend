package org.radarbase.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.stream.SensorStreamWorker
import org.radarbase.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4InterBeatInterval
import org.radarcns.stream.aggregator.NumericAggregate

/**
 * Kafka Stream for computing and aggregating Heart Rate values collected by Empatica E4.
 * It is used by converting inter beat interval (input) to heart rate.
 */
class E4HeartRateStream : SensorStreamWorker<ObservationKey, EmpaticaE4InterBeatInterval>() {
    override fun initialize() {
        defineWindowedSensorStream(
            "android_empatica_e4_inter_beat_interval",
            "android_empatica_e4_heart_rate",
        )
        config.setDefaultPriority(RadarPropertyHandler.Priority.LOW)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4InterBeatInterval>,
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateCustomNumeric(
            definition,
            kstream,
            { 60.0 / floatToDouble(it.interBeatInterval) },
            "heartRate",
        )
    }

    private fun floatToDouble(value: Float): Double {
        return value.toString().toDouble()
    }
}

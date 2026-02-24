package org.radarbase.kotlin.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.kotlin.stream.SensorStreamWorker
import org.radarbase.kotlin.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4InterBeatInterval
import org.radarcns.stream.aggregator.NumericAggregate

/**
 * Definition of Kafka Stream for aggregating Inter Beat Interval values collected by Empatica E4.
 */
class E4InterBeatIntervalStream : SensorStreamWorker<ObservationKey, EmpaticaE4InterBeatInterval>() {
    override fun initialize() {
        defineWindowedSensorStream("android_empatica_e4_inter_beat_interval")
        config.setDefaultPriority(RadarPropertyHandler.Priority.LOW)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4InterBeatInterval>
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateNumeric(
            definition, kstream, "interBeatInterval",
            EmpaticaE4InterBeatInterval.getClassSchema()
        )
    }
}

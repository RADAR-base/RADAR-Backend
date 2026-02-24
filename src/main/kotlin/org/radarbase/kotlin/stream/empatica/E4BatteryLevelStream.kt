package org.radarbase.kotlin.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.kotlin.stream.SensorStreamWorker
import org.radarbase.kotlin.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel
import org.radarcns.stream.aggregator.NumericAggregate

/**
 * Kafka Stream for aggregating data about Empatica E4 battery level.
 */
class E4BatteryLevelStream : SensorStreamWorker<ObservationKey, EmpaticaE4BatteryLevel>() {
    override fun initialize() {
        defineWindowedSensorStream("android_empatica_e4_battery_level")
        config.setDefaultPriority(RadarPropertyHandler.Priority.LOW)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4BatteryLevel>
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateNumeric(
            definition, kstream, "batteryLevel",
            EmpaticaE4BatteryLevel.getClassSchema()
        )
    }
}

package org.radarbase.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarConfigHandler
import org.radarbase.stream.SensorStreamWorker
import org.radarbase.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4Temperature
import org.radarcns.stream.aggregator.NumericAggregate

/**
 * Definition of Kafka Stream for aggregating temperature values collected by Empatica E4.
 */
class E4TemperatureStream : SensorStreamWorker<ObservationKey, EmpaticaE4Temperature>() {
    override fun initialize() {
        defineWindowedSensorStream("android_empatica_e4_temperature")
        config.setDefaultPriority(RadarConfigHandler.Priority.NORMAL)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4Temperature>,
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateNumeric(
            definition,
            kstream,
            "temperature",
            EmpaticaE4Temperature.getClassSchema(),
        )
    }
}

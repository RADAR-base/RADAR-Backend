package org.radarbase.kotlin.stream.empatica

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.kotlin.stream.SensorStreamWorker
import org.radarbase.kotlin.stream.StreamDefinition
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.empatica.EmpaticaE4ElectroDermalActivity
import org.radarcns.stream.aggregator.NumericAggregate

/**
 * Kafka Stream for aggregating data about electrodermal activity collected by Empatica E4.
 */
class E4ElectroDermalActivityStream : SensorStreamWorker<ObservationKey, EmpaticaE4ElectroDermalActivity>() {
    override fun initialize() {
        defineWindowedSensorStream("android_empatica_e4_electrodermal_activity")
        config.setDefaultPriority(RadarPropertyHandler.Priority.HIGH)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, EmpaticaE4ElectroDermalActivity>
    ): KStream<AggregateKey, NumericAggregate> {
        return aggregateNumeric(
            definition, kstream, "electroDermalActivity",
            EmpaticaE4ElectroDermalActivity.getClassSchema()
        )
    }
}

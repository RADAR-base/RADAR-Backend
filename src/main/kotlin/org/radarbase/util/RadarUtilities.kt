package org.radarbase.util

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.radarbase.stream.collector.AggregateListCollector
import org.radarbase.stream.collector.NumericAggregateCollector
import org.radarbase.stream.phone.PhoneUsageCollector
import org.radarbase.stream.phone.TemporaryPackageKey
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.stream.aggregator.AggregateList
import org.radarcns.stream.aggregator.NumericAggregate
import org.radarcns.stream.aggregator.PhoneUsageAggregate

/**
 * Kotlin counterpart of RadarUtilities. Provides utility transformations to Avro models.
 */
interface RadarUtilities {
    fun getWindowed(window: Windowed<ObservationKey>): AggregateKey
    fun getWindowedTuple(window: Windowed<TemporaryPackageKey>): AggregateKey
    fun listCollectorToAvro(
        window: Windowed<ObservationKey>,
        collector: AggregateListCollector,
    ): KeyValue<AggregateKey, AggregateList>

    fun numericCollectorToAvro(
        window: Windowed<ObservationKey>,
        collector: NumericAggregateCollector,
    ): KeyValue<AggregateKey, NumericAggregate>

    fun phoneCollectorToAvro(
        window: Windowed<TemporaryPackageKey>,
        collector: PhoneUsageCollector,
    ): KeyValue<AggregateKey, PhoneUsageAggregate>
}

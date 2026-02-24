package org.radarbase.kotlin.util

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.KeyValue.pair
import org.apache.kafka.streams.kstream.Window
import org.apache.kafka.streams.kstream.Windowed
import org.radarbase.kotlin.stream.phone.PhoneUsageCollector
import org.radarbase.kotlin.stream.phone.TemporaryPackageKey
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.stream.aggregator.AggregateList
import org.radarcns.stream.aggregator.NumericAggregate
import org.radarcns.stream.aggregator.PhoneUsageAggregate
import org.radarbase.stream.collector.AggregateListCollector
import org.radarbase.stream.collector.NumericAggregateCollector

class RadarUtilitiesImpl : RadarUtilities {
    override fun getWindowed(window: Windowed<ObservationKey>): AggregateKey {
        val key = window.key()
        val timeWindow: Window = window.window()
        return AggregateKey(
            key.projectId, key.userId, key.sourceId,
            timeWindow.start() / 1000.0, timeWindow.end() / 1000.0
        )
    }

    override fun getWindowedTuple(window: Windowed<TemporaryPackageKey>): AggregateKey {
        val key = window.key()
        val timeWindow: Window = window.window()
        return AggregateKey(
            key.projectId, key.userId, key.sourceId,
            timeWindow.start() / 1000.0, timeWindow.end() / 1000.0
        )
    }

    override fun phoneCollectorToAvro(
        window: Windowed<TemporaryPackageKey>,
        collector: PhoneUsageCollector
    ): KeyValue<AggregateKey, PhoneUsageAggregate> = pair(
        getWindowedTuple(window),
        PhoneUsageAggregate(
            window.key().packageName,
            collector.totalForegroundTime.toDouble(),
            collector.timesTurnedOn,
            collector.categoryName,
            collector.categoryNameFetchTime
        )
    )

    override fun listCollectorToAvro(
        window: Windowed<ObservationKey>,
        collector: AggregateListCollector
    ): KeyValue<AggregateKey, AggregateList> {
        val fields = collector.getCollectors().map { numericCollectorToAggregate(it) }
        return pair(getWindowed(window), AggregateList(fields))
    }

    override fun numericCollectorToAvro(
        window: Windowed<ObservationKey>,
        collector: NumericAggregateCollector
    ): KeyValue<AggregateKey, NumericAggregate> =
        pair(getWindowed(window), numericCollectorToAggregate(collector))

    private fun numericCollectorToAggregate(collector: NumericAggregateCollector): NumericAggregate =
        NumericAggregate(
            collector.name,
            collector.min, collector.max,
            collector.getSum(), collector.count.toInt(),
            collector.mean, collector.quartile
        )
}

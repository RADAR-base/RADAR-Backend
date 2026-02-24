package org.radarbase.stream.phone

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.stream.SensorStreamWorker
import org.radarbase.stream.StreamDefinition
import org.radarbase.util.serde.RadarSerdes
import org.radarcns.kafka.AggregateKey
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneUsageEvent
import org.radarcns.stream.aggregator.PhoneUsageAggregate
import java.time.Duration

/**
 * Created by piotrzakrzewski on 26/07/2017.
 */
class PhoneUsageAggregationStream : SensorStreamWorker<ObservationKey, PhoneUsageEvent>() {
    override fun initialize() {
        defineStream(
            "android_phone_usage_event_output",
            "android_phone_usage_event_aggregated",
            Duration.ofDays(1)
        )
        config.setDefaultPriority(RadarPropertyHandler.Priority.LOW)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, PhoneUsageEvent>
    ): KStream<AggregateKey, PhoneUsageAggregate> {
        return kstream.groupBy { k, v -> temporaryKey(k, v) }
            .windowedBy(definition.timeWindows)
            .aggregate(
                { PhoneUsageCollector() },
                { _, v, valueCollector -> valueCollector.update(v) },
                RadarSerdes.Companion.materialized<TemporaryPackageKey, PhoneUsageCollector>(
                    definition.stateStoreName,
                    RadarSerdes.Companion.getInstance().getPhoneUsageCollector()
                )
            )
            .toStream()
            .map(utilities::phoneCollectorToAvro)
    }

    private fun temporaryKey(key: ObservationKey, value: PhoneUsageEvent): TemporaryPackageKey {
        return TemporaryPackageKey(
            key.projectId, key.userId, key.sourceId,
            value.packageName
        )
    }
}

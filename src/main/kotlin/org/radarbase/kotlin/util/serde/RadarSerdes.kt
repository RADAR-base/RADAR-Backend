package org.radarbase.kotlin.util.serde

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.WindowStore
import org.radarbase.stream.collector.AggregateListCollector
import org.radarbase.stream.collector.NumericAggregateCollector
import org.radarbase.stream.phone.PhoneUsageCollector

class RadarSerdes private constructor() {
    private val numericCollector: Serde<NumericAggregateCollector> =
        RadarSerde(NumericAggregateCollector::class.java).getSerde()
    private val aggregateListCollector: Serde<AggregateListCollector> =
        RadarSerde(AggregateListCollector::class.java).getSerde()
    private val phoneUsageCollector: Serde<PhoneUsageCollector> =
        RadarSerde(PhoneUsageCollector::class.java).getSerde()

    fun getNumericAggregateCollector(): Serde<NumericAggregateCollector> = numericCollector
    fun getAggregateListCollector(): Serde<AggregateListCollector> = aggregateListCollector
    fun getPhoneUsageCollector(): Serde<PhoneUsageCollector> = phoneUsageCollector

    companion object {
        private val instance = RadarSerdes()
        @JvmStatic fun getInstance(): RadarSerdes = instance

        @JvmStatic
        fun <K, V> materialized(
            name: String,
            valueSerde: Serde<V>
        ): Materialized<K, V, WindowStore<Bytes, ByteArray>> =
            Materialized.`as`<K, V, WindowStore<Bytes, ByteArray>>(name).withValueSerde(valueSerde)
    }
}

package org.radarbase.kotlin.util.serde

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.WindowStore
import org.radarbase.stream.collector.AggregateListState
import org.radarbase.stream.collector.NumericAggregateState
import org.radarbase.stream.phone.PhoneUsageCollector

class RadarSerdes private constructor() {
    private val numericCollector: Serde<NumericAggregateState> =
        RadarSerde(NumericAggregateState::class.java).getSerde()
    private val aggregateListCollector: Serde<AggregateListState> =
        RadarSerde(AggregateListState::class.java).getSerde()
    private val phoneUsageCollector: Serde<PhoneUsageCollector> =
        RadarSerde(PhoneUsageCollector::class.java).getSerde()

    fun getNumericAggregateCollector(): Serde<NumericAggregateState> = numericCollector
    fun getAggregateListCollector(): Serde<AggregateListState> = aggregateListCollector
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

package org.radarbase.stream.phone

import org.apache.kafka.streams.kstream.KStream
import org.radarbase.config.RadarConfigHandler
import org.radarbase.stream.SensorStreamWorker
import org.radarbase.stream.StreamDefinition
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneUsageEvent
import org.slf4j.LoggerFactory

class PhoneUsageStream : SensorStreamWorker<ObservationKey, PhoneUsageEvent>() {
    private val playStoreLookup: PlayStoreLookup = PlayStoreLookup(CACHE_TIMEOUT.toLong(), MAX_CACHE_SIZE)

    override fun initialize() {
        defineSensorStream("android_phone_usage_event")
        config.setDefaultPriority(RadarConfigHandler.Priority.LOW)
    }

    override fun implementStream(
        definition: StreamDefinition,
        kstream: KStream<ObservationKey, PhoneUsageEvent>,
    ): KStream<ObservationKey, PhoneUsageEvent> {
        return kstream.mapValues { value ->
            val packageName = value.packageName
            val category = playStoreLookup.lookupCategory(packageName)
            logger.info("Looked up {}: {}", packageName, category.categoryName)
            value.categoryName = category.categoryName
            value.categoryNameFetchTime = category.fetchTimeStamp
            value
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PhoneUsageStream::class.java)

        // 1 day until an item is refreshed
        private const val CACHE_TIMEOUT = 24 * 3600

        // Do not cache more than 1 million elements, for memory consumption reasons
        private const val MAX_CACHE_SIZE = 1_000_000
    }
}

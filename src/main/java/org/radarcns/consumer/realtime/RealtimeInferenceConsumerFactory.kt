package org.radarcns.consumer.realtime

import org.radarcns.config.ConfigRadar
import org.radarcns.config.RadarPropertyHandler
import org.radarcns.config.realtime.RealtimeConsumerConfig
import org.radarcns.monitor.CombinedKafkaMonitor
import org.radarcns.monitor.KafkaMonitor

/**
 * Factory class for [RealtimeInferenceConsumer]. There can be multiple consumers, each with
 * its own set of [org.radarcns.consumer.realtime.condition.Condition]s and [ ]s.
 */
object RealtimeInferenceConsumerFactory {
    @JvmStatic
    fun createConsumersFor(handler: RadarPropertyHandler): KafkaMonitor {
        return CombinedKafkaMonitor(
                handler.radarProperties.consumerConfigs.stream()
                        .map { c: RealtimeConsumerConfig ->
                            createConsumer(handler.radarProperties, c)
                        }
        )
    }

    private fun createConsumer(
            radar: ConfigRadar, consumerConfig: RealtimeConsumerConfig): KafkaMonitor {
        return RealtimeInferenceConsumer(
                "realtime-group-${consumerConfig.topic}-${consumerConfig.name}",
                "${consumerConfig.topic}-1",
                radar,
                consumerConfig)
    }
}
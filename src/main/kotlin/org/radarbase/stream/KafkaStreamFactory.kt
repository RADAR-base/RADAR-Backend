package org.radarbase.stream

import org.radarbase.config.BackendProcess
import org.radarbase.config.RadarBackendCliOptions
import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.SingleStreamConfig
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Stream

class KafkaStreamFactory(
    private val cliOptions: RadarBackendCliOptions,
    private val radarProperties: RadarConfigHandler,
) {
    fun createSensorStreams(): StreamMaster {
        val cliStreamTypes = cliOptions.subCommandArgs.orEmpty()
            .toHashSet().map { it.lowercase(Locale.US) }

        val streamConfigs = radarProperties.radarProperties.stream?.streamConfigs
            // Keep only StreamConfigs enabled via CLI. Use all StreamConfigs if no CLI args are given.
            ?.filter { s ->
                val name = s.streamClass?.name?.lowercase(Locale.US).orEmpty()
                cliStreamTypes.isEmpty() || cliStreamTypes.any { name.endsWith(it) }
            }
            .orEmpty()
            .stream()

        return streamMaster(streamConfigs)
    }

    private fun streamMaster(streamConfigs: Stream<out SingleStreamConfig>) =
        StreamMaster(radarProperties, streamConfigs)

    fun createStreamStatisticsStream(): BackendProcess {
        val streamConfigs = radarProperties.radarProperties.stream?.sourceStatistics?.stream()
            ?: run {
                logger.warn("Statistics monitor is not configured. Cannot start it.")
                return streamMaster(Stream.empty())
            }

        return streamMaster(streamConfigs)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaStreamFactory::class.java)
    }
}

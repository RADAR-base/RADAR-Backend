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
        val args = cliOptions.subCommandArgs
        val streamTypes = if (args != null && args.isNotEmpty()) {
            args.toHashSet()
        } else {
            emptySet()
        }

        val streamConfigs = radarProperties.radarProperties.stream!!.streamConfigs!!.stream().filter { s ->
            streamTypes.isEmpty() || streamTypes.any { n ->
                s.streamClass!!.name.lowercase(Locale.US).endsWith(n.lowercase(Locale.US))
            }
        }

        return streamMaster(streamConfigs)
    }

    private fun streamMaster(streamConfigs: Stream<out SingleStreamConfig>) =
        StreamMaster(radarProperties, streamConfigs)

    fun createStreamStatisticsStream(): BackendProcess {
        val streamConfigs = radarProperties.radarProperties.stream!!.sourceStatistics

        if (streamConfigs == null) {
            logger.warn("Statistics monitor is not configured. Cannot start it.")
            return streamMaster(Stream.empty())
        }

        return streamMaster(streamConfigs.stream())
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaStreamFactory::class.java)
    }
}

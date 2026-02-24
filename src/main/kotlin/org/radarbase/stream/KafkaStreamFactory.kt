package org.radarbase.stream

import org.radarbase.config.RadarBackendOptions
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.config.SingleStreamConfig
import org.radarbase.config.SubCommand
import org.slf4j.LoggerFactory
import java.util.*
import java.util.stream.Stream

class KafkaStreamFactory(
    private val options: RadarBackendOptions,
    private val radarProperties: RadarPropertyHandler
) {
    fun createSensorStreams(): StreamMaster {
        val args = options.subCommandArgs
        val streamTypes = if (args != null && args.isNotEmpty()) {
            args.toHashSet()
        } else {
            emptySet()
        }

        val configs = radarProperties.radarProperties.stream!!.streamConfigs!!.stream()
            .filter { s ->
                streamTypes.isEmpty() || streamTypes.any { n ->
                    s.streamClass!!.name.lowercase(Locale.US).endsWith(n.lowercase(Locale.US))
                }
            }

        return master(configs)
    }

    private fun master(configs: Stream<out SingleStreamConfig>): StreamMaster {
        return StreamMaster(radarProperties, configs)
    }

    fun createStreamStatistics(): SubCommand {
        val configs = radarProperties.radarProperties.stream!!.sourceStatistics

        if (configs == null) {
            logger.warn("Statistics monitor is not configured. Cannot start it.")
            return master(Stream.empty())
        }

        return master(configs.stream())
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaStreamFactory::class.java)
    }
}

package org.radarbase

import org.apache.commons.cli.ParseException
import org.radarbase.config.BackendProcess
import org.radarbase.config.RadarBackendCliOptions
import org.radarbase.config.RadarConfigHandler
import org.radarbase.monitor.KafkaMonitorFactory
import org.radarbase.producer.MockProducerCommand
import org.radarbase.stream.KafkaStreamFactory
import org.radarbase.util.RadarSingletonFactory
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.system.exitProcess

/**
 * Core class that initializes configurations and then start configured Kafka streams
 */
class RadarBackend(
    private val cliOptions: RadarBackendCliOptions,
    private val radarConfigHandler: RadarConfigHandler = createPropertyHandler(cliOptions),
) {

    private lateinit var process: BackendProcess

    init {
        logger.info("Configuration successfully updated")
        logger.info("radar.yml configuration: {}", radarConfigHandler.radarProperties)
    }

    /**
     * Starts streams and sets a ShutdownHook to close streams while closing the application
     */
    fun run() {
        try {
            start()
        } catch (ex: IOException) {
            logger.error("FATAL ERROR! The current instance cannot start", ex)
            exitProcess(1)
        } catch (ex: InterruptedException) {
            logger.error("The current instance was interrupted", ex)
        }

        Runtime.getRuntime().addShutdownHook(
            Thread {
                try {
                    shutdown()
                } catch (ex: Exception) {
                    logger.error("Impossible to finalise the shutdown hook", ex)
                }
            },
        )
    }

    /**
     * Start here all needed StreamMasters
     *
     * @throws IOException if the command failed to start up
     * @throws InterruptedException if the command was interrupted
     */
    @Throws(IOException::class, InterruptedException::class)
    fun start() {
        logger.info("STARTING")

        process = createProcess()
        process.start()

        logger.info("STARTED")
    }

    /**
     * Stop here all commands started inside [start].
     *
     * @throws IOException if the command failed to shut down
     * @throws InterruptedException if the command was interrupted before completely shutting down
     */
    @Throws(InterruptedException::class, IOException::class)
    fun shutdown() {
        logger.info("SHUTTING DOWN")

        process.shutdown()

        logger.info("FINISHED")
    }

    @Throws(IOException::class)
    fun createProcess(): BackendProcess {
        val cliSubCommand = cliOptions.subCommand ?: "stream"
        return when (cliSubCommand) {
            "stream" -> KafkaStreamFactory(cliOptions, radarConfigHandler).createSensorStreams()
            "statistics" -> KafkaStreamFactory(cliOptions, radarConfigHandler).createStreamStatisticsStream()
            "monitor" -> KafkaMonitorFactory(cliOptions, radarConfigHandler).createMonitor()
            "mock" -> MockProducerCommand(cliOptions, radarConfigHandler)
            else -> throw IllegalArgumentException("Unknown subcommand ${cliOptions.subCommand}")
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RadarBackend::class.java)

        @Throws(IOException::class)
        private fun createPropertyHandler(options: RadarBackendCliOptions): RadarConfigHandler {
            val properties = RadarSingletonFactory.radarConfigHandler
            properties.load(options.propertyPath)
            return properties
        }

        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val options = RadarBackendCliOptions.parse(args)
                val backend = RadarBackend(options)
                backend.run()
            } catch (ex: ParseException) {
                logger.error(
                    "Cannot parse arguments {}. Valid options are:\n{}",
                    args.contentToString(),
                    RadarBackendCliOptions.OPTIONS,
                )
                exitProcess(1)
            } catch (ex: Exception) {
                logger.error("Failed to run command", ex)
                exitProcess(1)
            }
        }
    }
}

package org.radarbase.kotlin

import org.apache.commons.cli.ParseException
import org.radarbase.kotlin.config.RadarBackendOptions
import org.radarbase.kotlin.config.RadarPropertyHandler
import org.radarbase.kotlin.config.SubCommand
import org.radarbase.kotlin.monitor.KafkaMonitorFactory
import org.radarbase.kotlin.producer.MockProducerCommand
import org.radarbase.kotlin.stream.KafkaStreamFactory
import org.radarbase.kotlin.util.RadarSingletonFactory
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.system.exitProcess

/**
 * Core class that initialises configurations and then start all needed Kafka streams
 */
class RadarBackend(
    private val options: RadarBackendOptions,
    private val radarPropertyHandler: RadarPropertyHandler
) {

    constructor(options: RadarBackendOptions) : this(options, createPropertyHandler(options))

    private lateinit var command: SubCommand

    init {
        logger.info("Configuration successfully updated")
        logger.info("radar.yml configuration: {}", radarPropertyHandler.radarProperties)
    }

    /**
     * It starts streams and sets a ShutdownHook to close streams while closing the application
     */
    fun application() {
        try {
            start()
        } catch (ex: IOException) {
            logger.error("FATAL ERROR! The current instance cannot start", ex)
            exitProcess(1)
        } catch (ex: InterruptedException) {
            logger.error("The current instance was interrupted", ex)
        }

        Runtime.getRuntime().addShutdownHook(Thread {
            try {
                shutdown()
            } catch (ex: Exception) {
                logger.error("Impossible to finalise the shutdown hook", ex)
            }
        })
    }

    /**
     * Start here all needed StreamMaster
     *
     * @throws IOException if the command failed to start up
     * @throws InterruptedException if the command was interrupted
     */
    @Throws(IOException::class, InterruptedException::class)
    fun start() {
        logger.info("STARTING")

        command = createCommand()
        command.start()

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

        command.shutdown()

        logger.info("FINISHED")
    }

    @Throws(IOException::class)
    fun createCommand(): SubCommand {
        val subCommand = options.subCommand ?: "stream"
        return when (subCommand) {
            "stream" -> KafkaStreamFactory(options, radarPropertyHandler)
                .createSensorStreams()

            "statistics" -> KafkaStreamFactory(options, radarPropertyHandler)
                .createStreamStatistics()

            "monitor" -> KafkaMonitorFactory(options, radarPropertyHandler).createMonitor()
            "mock" -> MockProducerCommand(options, radarPropertyHandler)
            else -> throw IllegalArgumentException("Unknown subcommand ${options.subCommand}")
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RadarBackend::class.java)

        @Throws(IOException::class)
        private fun createPropertyHandler(options: RadarBackendOptions): RadarPropertyHandler {
            val properties = RadarSingletonFactory.radarPropertyHandler
            properties.load(options.propertyPath)
            return properties
        }

        @JvmStatic
        fun main(args: Array<String>) {
            try {
                val options = RadarBackendOptions.parse(args)
                val backend = RadarBackend(options)
                backend.application()
            } catch (ex: ParseException) {
                logger.error(
                    "Cannot parse arguments {}. Valid options are:\n{}",
                    args.contentToString(), RadarBackendOptions.OPTIONS
                )
                exitProcess(1)
            } catch (ex: Exception) {
                logger.error("Failed to run command", ex)
                exitProcess(1)
            }
        }
    }
}
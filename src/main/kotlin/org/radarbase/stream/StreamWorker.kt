package org.radarbase.stream

import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.SingleStreamConfig
import java.util.stream.Stream

/**
 * Interface for any Kafka stream worker in the RADAR-Backend.
 * It defines the core lifecycle and configuration methods for processing data from Kafka topics.
 */
interface StreamWorker {
    fun start()
    fun configure(
        streamMaster: StreamMaster,
        properties: RadarConfigHandler,
        singleConfig: SingleStreamConfig,
    )

    fun getStreamDefinitions(): Stream<StreamDefinition>
    fun shutdown()
}

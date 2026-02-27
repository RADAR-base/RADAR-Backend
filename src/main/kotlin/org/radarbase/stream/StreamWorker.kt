package org.radarbase.stream

import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.SingleStreamConfig
import java.util.stream.Stream

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

package org.radarbase.kotlin.stream

import org.radarbase.kotlin.config.RadarPropertyHandler
import org.radarbase.kotlin.config.SingleStreamConfig
import java.util.stream.Stream

interface StreamWorker {
    fun start()
    fun configure(
        streamMaster: StreamMaster,
        properties: RadarPropertyHandler,
        singleConfig: SingleStreamConfig
    )
    fun getStreamDefinitions(): Stream<StreamDefinition>
    fun shutdown()
}

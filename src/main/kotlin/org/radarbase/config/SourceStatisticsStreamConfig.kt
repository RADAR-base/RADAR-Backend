package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty
import org.radarbase.stream.statistics.SourceStatisticsStream

data class SourceStatisticsStreamConfig(
    var name: String? = null,

    var topics: List<String>? = null,

    @param:JsonProperty("output_topic")
    var outputTopic: String = "source_statistics",

    @param:JsonProperty("max_batch_size")
    var maxBatchSize: Int = 1000,

    @param:JsonProperty("flush_timeout")
    var flushTimeout: Long = 60_000L,
) : SingleStreamConfig() {
    init {
        streamClass = SourceStatisticsStream::class.java
    }
}

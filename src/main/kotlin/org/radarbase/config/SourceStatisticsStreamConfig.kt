package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty
import org.radarbase.stream.statistics.SourceStatisticsStream

class SourceStatisticsStreamConfig : SingleStreamConfig() {
    var name: String? = null

    var topics: List<String>? = null

    @JsonProperty("output_topic")
    var outputTopic: String = "source_statistics"

    @JsonProperty("max_batch_size")
    var maxBatchSize: Int = 1000

    @JsonProperty("flush_timeout")
    var flushTimeout: Long = 60_000L

    init {
        streamClass = SourceStatisticsStream::class.java
    }
}

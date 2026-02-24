package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import org.radarbase.stream.TimeWindowMetadata
import java.time.Duration
import java.util.*

@Suppress("PMD.ImmutableField")
class StreamConfig {
    private val timeWindowCommitInterval: MutableMap<TimeWindowMetadata, Duration> = EnumMap(TimeWindowMetadata::class.java)

    @JsonIgnore
    private val priorityThreads: MutableMap<RadarPropertyHandler.Priority, Int> = EnumMap(RadarPropertyHandler.Priority::class.java)

    @JsonProperty("min_commit_interval")
    var minCommitInterval: Long = 10

    @JsonProperty("max_commit_interval")
    var maxCommitInterval: Long = Duration.ofHours(3).seconds

    @JsonProperty("time_window_commit_interval_multiplier")
    var timeWindowCommitIntervalMultiplier: Float = 0.5f

    @JsonProperty
    var properties: Map<String, String>? = null
        get() = field ?: emptyMap()

    @JsonProperty("streams")
    var streamConfigs: List<SingleStreamConfig>? = null

    @JsonProperty("source_statistics")
    var sourceStatistics: List<SourceStatisticsStreamConfig>? = null

    init {
        priorityThreads[RadarPropertyHandler.Priority.LOW] = 1
        priorityThreads[RadarPropertyHandler.Priority.NORMAL] = 2
        priorityThreads[RadarPropertyHandler.Priority.HIGH] = 4
    }

    @get:JsonGetter("threads_per_priority")
    var threadsPerPriority: Map<String, Int>
        get() = priorityThreads.entries.associate { it.key.param to it.value }
        @JsonSetter("threads_per_priority")
        set(streamPriority) {
            streamPriority.values.forEach { v ->
                require(!(v < 1)) { "Stream priorities cannot be smaller than 1" }
            }
            priorityThreads.putAll(streamPriority.entries.associate { (key, value) ->
                RadarPropertyHandler.Priority.valueOf(key.uppercase(Locale.US)) to value
            })
        }

    @JsonIgnore
    fun getCommitIntervalForTimeWindow(w: TimeWindowMetadata): Duration {
        if (timeWindowCommitInterval.isEmpty()) {
            timeWindowCommitInterval.putAll(TimeWindowMetadata.values().associateWith { t ->
                val base = (timeWindowCommitIntervalMultiplier * t.intervalInMilliSec / 1000.0).toLong()
                Duration.ofSeconds(maxCommitInterval.coerceAtMost(base.coerceAtMost(maxCommitInterval)))
            })
        }
        return timeWindowCommitInterval[w]!!
    }

    fun threadsByPriority(level: RadarPropertyHandler.Priority): Int = priorityThreads[level] ?: 1
}

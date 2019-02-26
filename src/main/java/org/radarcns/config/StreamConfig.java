package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.stream.TimeWindowMetadata;

// POJO class
@SuppressWarnings("PMD.ImmutableField")
public class StreamConfig {
    private final Map<TimeWindowMetadata, Duration> timeWindowCommitInterval =
            new EnumMap<>(TimeWindowMetadata.class);

    @JsonIgnore
    private final Map<Priority, Integer> priorityThreads;
    @JsonProperty("min_commit_interval")
    private long minCommitInterval = 10;
    @JsonProperty("max_commit_interval")
    private long maxCommitInterval = Duration.ofHours(3).getSeconds();
    @JsonProperty("time_window_commit_interval_multiplier")
    private float timeWindowCommitIntervalMultiplier = 0.5f;
    @JsonProperty
    private Map<String, String> properties;
    @JsonProperty("streams")
    private List<SingleStreamConfig> streamConfigs;

    @JsonProperty
    private boolean useReservoirSampling = false;

    @JsonProperty("source_statistics")
    private List<SourceStatisticsStreamConfig> sourceStatistics;


    public StreamConfig() {
        priorityThreads = new EnumMap<>(Priority.class);
        priorityThreads.put(Priority.LOW, 1);
        priorityThreads.put(Priority.NORMAL, 2);
        priorityThreads.put(Priority.HIGH, 4);
    }

    public long getMinCommitInterval() {
        return minCommitInterval;
    }

    public long getMaxCommitInterval() {
        return maxCommitInterval;
    }

    public float getTimeWindowCommitIntervalMultiplier() {
        return timeWindowCommitIntervalMultiplier;
    }

    public Map<String, String> getProperties() {
        return properties != null ? properties : Collections.emptyMap();
    }

    public Stream<SingleStreamConfig> getStreamConfigs() {
        return streamConfigs.stream()
                .peek(s -> s.setDefaultUseReservoirSampling(useReservoirSampling));
    }

    @JsonGetter("threads_per_priority")
    public Map<String, Integer> getThreadsPerPriority() {
        return priorityThreads.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getParam(), Map.Entry::getValue));
    }

    @JsonSetter("threads_per_priority")
    public void setThreadsPerPriority(Map<String, Integer> streamPriority) {
        streamPriority.values().forEach(v -> {
            if (v == null || v < 1) {
                throw new IllegalArgumentException("Stream priorities cannot be smaller than 1");
            }
        });
        this.priorityThreads.putAll(streamPriority.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> Priority.valueOf(e.getKey().toUpperCase(Locale.US)),
                        Map.Entry::getValue)));
    }

    @JsonIgnore
    public Duration getCommitIntervalForTimeWindow(TimeWindowMetadata w) {
        if (timeWindowCommitInterval.isEmpty()) {
            timeWindowCommitInterval.putAll(Stream.of(TimeWindowMetadata.values())
                    .collect(Collectors.toMap(Function.identity(),
                            t -> {
                                long base = (long) (timeWindowCommitIntervalMultiplier
                                        * t.getIntervalInMilliSec() / 1000.0);
                                return Duration.ofSeconds(
                                        Math.min(maxCommitInterval,
                                                Math.min(base, maxCommitInterval)));
                            })));
        }
        return timeWindowCommitInterval.get(w);
    }

    public int threadsByPriority(Priority level) {
        return priorityThreads.get(level);
    }

    public List<SourceStatisticsStreamConfig> getSourceStatistics() {
        return sourceStatistics;
    }

    public boolean isUseReservoirSampling() {
        return useReservoirSampling;
    }

    public void setUseReservoirSampling(boolean useReservoirSampling) {
        this.useReservoirSampling = useReservoirSampling;
    }
}

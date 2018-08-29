package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.radarcns.stream.statistics.SourceStatisticsStream;

public class SourceStatisticsStreamConfig extends SingleStreamConfig {
    private String name;

    private List<String> topics;

    @JsonProperty("output_topic")
    private String outputTopic = "source_statistics";

    @JsonProperty("max_batch_size")
    private int maxBatchSize = 1000;

    @JsonProperty("flush_timeout")
    private long flushTimeout = 60_000L;

    public SourceStatisticsStreamConfig() {
        setStreamClass(SourceStatisticsStream.class);
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public long getFlushTimeout() {
        return flushTimeout;
    }

    public void setFlushTimeout(long flushTimeout) {
        this.flushTimeout = flushTimeout;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

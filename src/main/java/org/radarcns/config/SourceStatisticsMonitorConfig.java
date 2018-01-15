package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SourceStatisticsMonitorConfig {
    private List<String> topics;

    @JsonProperty("output_topic")
    private String outputTopic = "monitor_statistics";

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
}

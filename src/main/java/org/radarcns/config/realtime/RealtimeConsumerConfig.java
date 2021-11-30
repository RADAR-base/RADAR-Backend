package org.radarcns.config.realtime;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class RealtimeConsumerConfig {

  private String name;

  private String topic;

  @JsonProperty("conditions")
  private List<ConditionConfig> conditionConfigs;

  @JsonProperty("actions")
  private List<ActionConfig> actionConfigs;

  @JsonProperty("consumer_properties")
  private Map<String, String> consumerProperties;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public List<ConditionConfig> getConditionConfigs() {
    return conditionConfigs;
  }

  public void setConditionConfigs(List<ConditionConfig> conditionConfigs) {
    this.conditionConfigs = conditionConfigs;
  }

  public List<ActionConfig> getActionConfigs() {
    return actionConfigs;
  }

  public void setActionConfigs(List<ActionConfig> actionConfigs) {
    this.actionConfigs = actionConfigs;
  }

  public Map<String, String> getConsumerProperties() {
    return consumerProperties;
  }

  public void setConsumerProperties(Map<String, String> consumerProperties) {
    this.consumerProperties = consumerProperties;
  }
}

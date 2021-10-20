package org.radarcns.consumer.realtime.condition;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.config.realtime.ConditionConfig;

/**
 * Reads the JsonPath specification from the configuration file, provided to this class in {@link
 * ConditionConfig}.
 */
public class LocalJsonPathCondition extends JsonPathCondition {

  public static final String NAME = "LocalJsonPathCondition";

  private final String jsonPath;

  public LocalJsonPathCondition(ConditionConfig conditionConfig) {
    this.jsonPath = (String) conditionConfig.getProperties().get("jsonpath");
    if (jsonPath == null) {
      throw new IllegalArgumentException(
          "The 'jsonpath' property needs to be specified when "
              + "using the LocalJsonPathCondition.");
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Boolean isTrueFor(ConsumerRecord<?, ?> record) throws IOException {
    return evaluateJsonPath(record, jsonPath);
  }
}

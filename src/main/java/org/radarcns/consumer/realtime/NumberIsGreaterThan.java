package org.radarcns.consumer.realtime;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.config.realtime.ConditionConfig;

public class NumberIsGreaterThan implements Condition {

  public static final String NAME = "numberIsGreaterThan";
  private final String field;
  private final Double value;

  public NumberIsGreaterThan(ConditionConfig conditionConfig) {
    this.field = (String) conditionConfig.getProperties().getOrDefault("field", "score");
    this.value = (Double) conditionConfig.getProperties().get("value");
    if (value == null) {
      throw new IllegalArgumentException(
          "Please configure the value for the condition in the configuration");
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Boolean isTrueFor(ConsumerRecord<?, ?> record) {
    Object val = ((GenericRecord) record.value()).get(field);
    return val instanceof Number && ((Number) val).doubleValue() > value;
  }
}

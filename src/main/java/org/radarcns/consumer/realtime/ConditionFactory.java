package org.radarcns.consumer.realtime;

import org.radarcns.config.realtime.ConditionConfig;

public class ConditionFactory {

  public static Condition getConditionFor(ConditionConfig conditionConfig) {
    switch (conditionConfig.getName()) {
      case NumberIsGreaterThan.NAME:
        return new NumberIsGreaterThan(conditionConfig);
      default:
        throw new IllegalArgumentException(
            "The specified condition with name " + conditionConfig.getName() + " is not correct.");
    }
  }
}

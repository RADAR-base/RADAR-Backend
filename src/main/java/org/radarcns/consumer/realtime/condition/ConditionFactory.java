package org.radarcns.consumer.realtime.condition;

import org.radarcns.config.realtime.ConditionConfig;

public class ConditionFactory {

  public static Condition getConditionFor(ConditionConfig conditionConfig) {
    switch (conditionConfig.getName()) {
      case LocalJsonPathCondition.NAME:
        return new LocalJsonPathCondition(conditionConfig);
      default:
        throw new IllegalArgumentException(
            "The specified condition with name " + conditionConfig.getName() + " is not correct.");
    }
  }
}

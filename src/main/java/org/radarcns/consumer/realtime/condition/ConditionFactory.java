package org.radarcns.consumer.realtime.condition;

import org.radarcns.config.realtime.ConditionConfig;

/**
 * Factory class for {@link Condition}s. It instantiates conditions based on the configuration
 * provided for the given consumer.
 */
@SuppressWarnings("PMD.ClassNamingConventions")
public final class ConditionFactory {

  private ConditionFactory() {
  }

  @SuppressWarnings("PMD.TooFewBranchesForASwitchStatement")
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

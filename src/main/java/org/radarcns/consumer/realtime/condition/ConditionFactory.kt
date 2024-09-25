package org.radarcns.consumer.realtime.condition

import org.radarcns.config.realtime.ConditionConfig

/**
 * Factory class for [Condition]s. It instantiates conditions based on the configuration
 * provided for the given consumer.
 */
object ConditionFactory {
    @JvmStatic
    fun getConditionFor(conditionConfig: ConditionConfig): Condition {
        return when (conditionConfig.name) {
            LocalJsonPathCondition.NAME -> LocalJsonPathCondition(conditionConfig)
            else -> throw IllegalArgumentException(
                    "The specified condition with name " + conditionConfig.name + " is not correct.")
        }
    }
}
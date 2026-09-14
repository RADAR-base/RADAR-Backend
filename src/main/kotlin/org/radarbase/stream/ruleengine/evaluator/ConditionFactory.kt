package org.radarbase.stream.ruleengine.evaluator

import org.radarbase.config.intervention.ConditionConfig
import org.radarbase.config.intervention.ExpressionType

/**
 * Factory class for [ConditionEvaluator]s. It instantiates conditions based on the configuration
 * provided for the given consumer.
 */
object ConditionFactory {
    @JvmStatic
    fun getConditionEvaluator(conditionConfig: ConditionConfig): ConditionEvaluator {
        return when (conditionConfig.type) {
            CelConditionEvaluator.TYPE_NAME -> CelConditionEvaluator
            else -> throw IllegalArgumentException(
                "The specified condition with type " + conditionConfig.type + " is not correct. Implementations are: " + ExpressionType.entries.joinToString(
                    ", "
                ) { it.name })
        }
    }
}
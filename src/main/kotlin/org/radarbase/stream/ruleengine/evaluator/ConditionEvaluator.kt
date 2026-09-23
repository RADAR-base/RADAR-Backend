package org.radarbase.stream.ruleengine.evaluator

/**
 * TODO update package ref
 * A condition can be defined as any predicate on the incoming data that must be true before the
 * [org.radarbase.consumer.realtime.action.Action]s can be triggered.
 */
interface ConditionEvaluator {

    fun isTrueFor(key: Map<*, *>, value: Map<*, *>, expression: String): Boolean
}

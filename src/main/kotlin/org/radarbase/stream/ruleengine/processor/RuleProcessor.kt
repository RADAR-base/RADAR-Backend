package org.radarbase.stream.ruleengine.processor

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.api.ContextualProcessor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.radarbase.config.intervention.ActionConfig
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.evaluator.ConditionFactory
import org.radarbase.stream.ruleengine.evaluator.toCelCompatible
import org.slf4j.LoggerFactory

class RuleProcessor(
    val globalStoreName: String,
) : ContextualProcessor<GenericRecord, GenericRecord, GenericRecord, ActionConfig>() {

    private lateinit var groupedRulesStore: ReadOnlyKeyValueStore<String, RuleGroup>

    override fun init(context: ProcessorContext<GenericRecord, ActionConfig>) {
        super.init(context)
        groupedRulesStore = context().getStateStore(globalStoreName)
    }

    override fun process(record: Record<GenericRecord, GenericRecord>) {
        val recordKeyAvro = record.key()
        val recordValueAvro = record.value()

        if (recordKeyAvro == null || recordValueAvro == null) return

        val topic = context().recordMetadata().get().topic()


        @Suppress("UNCHECKED_CAST")
        val recordKey = recordKeyAvro.toCelCompatible() as Map<String, Any?>

        @Suppress("UNCHECKED_CAST")
        val recordValue = recordValueAvro.toCelCompatible() as Map<String, Any?>

        logger.debug("Processing record from topic {} with key {} and value {}", topic, recordKey, recordValue)

        val projectId = recordKey["projectId"] as? String
        val userId = recordKey["userId"] as? String

        val applicableScopeKeys = buildSet {
            add(ScopePrefix.GLOBAL.scope)
            projectId?.let { add("${ScopePrefix.PROJECT.scope}$it") }
            userId?.let { add("${ScopePrefix.USER.scope}$it") }
        }

        val matchedRules = applicableScopeKeys
            .mapNotNull { groupedRulesStore.get(it) }
            .flatMap { it.rules.entries }
            .distinctBy { it.key }

        if (matchedRules.isEmpty()) {
            logger.debug("No rules found for topic {} with scopes {}", topic, applicableScopeKeys)
            return
        }

        matchedRules.forEach { (storeKey, interventionConfig) ->
            evaluateRule(storeKey, interventionConfig, applicableScopeKeys, recordKey, recordValue, recordKeyAvro, record.timestamp())
        }
    }

    private fun evaluateRule(
        storeKey: String,
        interventionConfig: InterventionConfig,
        applicableScopeKeys: Set<String>,
        recordKey: Map<String, Any?>,
        recordValue: Map<String, Any?>,
        originalKey: GenericRecord,
        timestamp: Long,
    ) {
        val applicableConditions = interventionConfig.conditionConfigs.filter { condition ->
            condition.toScopeKeys().any { it in applicableScopeKeys }
        }

        if (applicableConditions.isEmpty()) return

        val matched = try {
            applicableConditions.all { condition ->
                val evaluator = ConditionFactory.getConditionEvaluator(condition)
                evaluator.isTrueFor(recordKey, recordValue, condition.expression)
            }
        } catch (e: Exception) {
            logger.warn("Failed to evaluate rule {}: {}", storeKey, e.message)
            false
        }

        if (!matched) return

        interventionConfig.actionConfigs
            .filter { action -> action.toScopeKeys().any { it in applicableScopeKeys } }
            .forEach { action -> context().forward(Record(originalKey, action, timestamp)) }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleProcessor::class.java)
    }
}

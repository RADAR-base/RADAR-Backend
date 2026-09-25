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

        val recordKey = recordKeyAvro.toCelCompatible()
        val recordValue = recordValueAvro.toCelCompatible()

        logger.debug("Processing record from topic {} with key {} and value {}", topic, recordKey, recordValue)

        val rule = groupedRulesStore.get(ScopePrefix.GLOBAL.scope)
        // TODO: do something with Scope

        rule.rules.forEach { (_, interventionConfig) ->
            val result = evaluateRule(interventionConfig, recordKey, recordValue)
            if (result) {
                interventionConfig.actionConfigs
                    .forEach { action -> context().forward(Record(null, action, record.timestamp())) }
                // TODO: update the tests
                // TODO: Can we send to different output streams
            }
        }

    }

    private fun evaluateRule(
        interventionConfig: InterventionConfig,
        recordKey: Map<String, Any?>,
        recordValue: Map<String, Any?>,
    ):Boolean {
        val applicableConditions = interventionConfig.conditionConfigs
        try {
            return applicableConditions.any { condition ->
                val evaluator = ConditionFactory.getConditionEvaluator(condition)
                evaluator.isTrueFor(recordKey, recordValue, condition.expression)
            }
        } catch (e: Exception) {
            logger.warn("Failed to evaluate rule {}: {}", interventionConfig.name ,e.message)
            return false
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleProcessor::class.java)
    }

    fun GenericRecord.toCelCompatible(): Map<String, Any> =
        this.schema.fields.associate { it.name() to this.get(it.pos()).toCelCompatible() }

    fun Any.toCelCompatible(): Any = when (this) {
        is CharSequence -> this.toString() // Converts Avro Utf8 to java.lang.String
        is List<*> -> this.map { it?.toCelCompatible() }
        is Map<*, *> -> this.entries.associate { it.key.toString() to it.value?.toCelCompatible() }
        else -> this
    }
}



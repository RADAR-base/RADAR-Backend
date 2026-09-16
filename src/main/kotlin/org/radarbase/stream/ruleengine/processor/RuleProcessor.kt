package org.radarbase.stream.ruleengine.processor

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.radarbase.config.intervention.ActionConfig
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.evaluator.ConditionFactory
import org.slf4j.LoggerFactory

class RuleProcessor(
    val globalStoreName: String,
) : org.apache.kafka.streams.processor.api.ContextualProcessor<GenericRecord, GenericRecord, RuleKey, InterventionConfig>() {

    private lateinit var groupedRulesStore: ReadOnlyKeyValueStore<String, RuleGroup>

    override fun init(context: ProcessorContext<RuleKey, InterventionConfig>) {
        super.init(context)
        groupedRulesStore = context().getStateStore(globalStoreName)
    }

    override fun process(record: Record<GenericRecord, GenericRecord>) {
        val topic = context().recordMetadata().get().topic()
        val recordKeyAvro = record.key()
        val recordValueAvro = record.value()

        if (recordKeyAvro == null || recordValueAvro == null) return

        val recordKey = recordKeyAvro.toMap()
        val recordValue = recordValueAvro.toMap()

        logger.debug("Processing record from topic {} with key {} and value {}", topic, recordKey, recordValue)

        val project = recordKey["project"]
        val subject = recordKey["user"]
        val source = recordKey["source"]
        val lookupKey = "$topic:$project"
        val ruleGroup = groupedRulesStore.get(lookupKey)

        if (ruleGroup == null || ruleGroup.rules.isEmpty()) {
            logger.debug("No rules found for project {} on topic {}", project, topic)
            return
        }

        // Look at global intervention configs first.
        groupedRulesStore[ScopePrefix.GLOBAL.scope]?.also {
            it.rules.forEach { (_, interventionConfig) ->
                interventionConfig.conditionConfigs.forEach { conditionConfig ->
                    val evaluator = ConditionFactory.getConditionEvaluator(conditionConfig)
                    if (evaluator.isTrueFor(record, conditionConfig.expression)) {
                        interventionConfig.actionConfigs.forEach { actionConfig ->
                            actionConfig.forward()
                        }
                    }
                }
            }
        }
    }

    //    private fun evaluateRule(key: RuleKey, value: RuleValue, payload: Map<String, Any?>, timestamp: Long) {
    //        try {
    //            val program = programCache.computeIfAbsent(key.name) { condition ->
    //                val ast = astCache.computeIfAbsent(condition) {
    //                    celCompiler.compile(it).ast
    //                }
    //                celRuntime.createProgram(ast)
    //            }
    //
    //            val result = program.eval(mapOf("payload" to payload))
    //            // Send ActionConfigs to output topic if condition is true
    //            if (result is Boolean && result) {
    //                value.actionConfigs.forEach {
    //                    val forwardRecord: Record<RuleKey, ActionConfig> = Record(key, it, timestamp)
    //                    context().forward(forwardRecord)
    //                }
    //            }
    //        } catch (e: Exception) {
    //            logger.error("Error evaluating rule for topic {}: {}", key.topicName, e.message)
    //        }
    //    }

    fun GenericRecord.toMap(): Map<String, Any?> = this.schema.fields.associate { it.name() to this.get(it.pos()) }

    fun ActionConfig.forward() {
        // TODO implement forwarding
//        context().forward<Record<RuleKey, ActionConfig>(null, this, timestamp))
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleProcessor::class.java)
    }
}

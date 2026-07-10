package org.radarbase.stream.ruleengine.processor

import dev.cel.common.CelAbstractSyntaxTree
import dev.cel.common.types.CelTypes
import dev.cel.compiler.CelCompiler
import dev.cel.compiler.CelCompilerFactory
import dev.cel.runtime.CelRuntime
import dev.cel.runtime.CelRuntimeFactory
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.radarbase.stream.ruleengine.domain.ActionConfig
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.RuleValue
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

class RuleProcessor(
    val globalStoreName: String
) : org.apache.kafka.streams.processor.api.ContextualProcessor<GenericRecord, GenericRecord, RuleKey, ActionConfig>() {

    private lateinit var groupedRulesStore: ReadOnlyKeyValueStore<String, RuleGroup>
    private val celCompiler: CelCompiler = CelCompilerFactory
        .standardCelCompilerBuilder()
        .addVar("payload", CelTypes.DYN)
        .build()
    private val celRuntime: CelRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build()
    private val astCache = ConcurrentHashMap<String, CelAbstractSyntaxTree>()
    private val programCache = ConcurrentHashMap<String, CelRuntime.Program>()

    override fun init(context: ProcessorContext<RuleKey, ActionConfig>) {
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
        val lookupKey = "$topic:$project"
        val ruleGroup = groupedRulesStore.get(lookupKey)

        if (ruleGroup == null || ruleGroup.rules.isEmpty()) {
            logger.debug("No rules found for project {} on topic {}", project, topic)
            return
        }

        ruleGroup.rules.forEach { (ruleKey, ruleValue) ->
            logger.info("Matching rule found: topic={} project={} condition={}", topic, ruleKey.project, ruleKey.condition)
            evaluateRule(ruleKey, ruleValue, recordValue, record.timestamp())
        }
    }

    private fun evaluateRule(key: RuleKey, value: RuleValue, payload: Map<String, Any?>, timestamp: Long) {
        try {
            val program = programCache.computeIfAbsent(key.condition) { condition ->
                val ast = astCache.computeIfAbsent(condition) {
                    celCompiler.compile(it).ast
                }
                celRuntime.createProgram(ast)
            }

            val result = program.eval(mapOf("payload" to payload))
            if (result is Boolean && result) {
                value.actionConfigs.forEach {
                    val forwardRecord: Record<RuleKey, ActionConfig> = Record(key, it, timestamp)
                    context().forward(forwardRecord)
                }
            }
        } catch (e: Exception) {
            logger.error("Error evaluating rule for topic {}: {}", key.topicName, e.message)
        }
    }

    fun GenericRecord.toMap(): Map<String, Any?> = this.schema.fields.associate { it.name() to this.get(it.pos()) }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleProcessor::class.java)
    }
}

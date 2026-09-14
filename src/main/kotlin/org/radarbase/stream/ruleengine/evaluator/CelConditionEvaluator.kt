package org.radarbase.stream.ruleengine.evaluator

import dev.cel.common.CelAbstractSyntaxTree
import dev.cel.common.types.CelTypes
import dev.cel.compiler.CelCompiler
import dev.cel.compiler.CelCompilerFactory
import dev.cel.runtime.CelRuntime
import dev.cel.runtime.CelRuntimeFactory
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.api.Record
import org.radarbase.config.intervention.ExpressionType
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

object CelConditionEvaluator : ConditionEvaluator {

    val TYPE_NAME = ExpressionType.CEL
    private val logger = LoggerFactory.getLogger(CelConditionEvaluator::class.java)
    const val KEY_MEMBER_NAME = "key"
    const val VALUE_MEMBER_NAME = "value"

    private val celCompiler: CelCompiler =
        CelCompilerFactory.standardCelCompilerBuilder().addVar(VALUE_MEMBER_NAME, CelTypes.DYN).build()
    private val celRuntime: CelRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build()
    private val astCache = ConcurrentHashMap<String, CelAbstractSyntaxTree>()
    private val programCache = ConcurrentHashMap<String, CelRuntime.Program>()

    override fun isTrueFor(record: Record<GenericRecord, GenericRecord>, expression: String): Boolean {

        val recordKey = record.key().toCelCompatible() as Map<*, *>
        val recordValue = record.value().toCelCompatible() as Map<*, *>

        logger.debug("Processing record from topic with key: {} and value {}", recordKey, recordValue)

        return getEvaluator(expression)?.eval(
            mapOf(
                KEY_MEMBER_NAME to recordKey,
                VALUE_MEMBER_NAME to recordValue
            )
        ).let {
            it is Boolean && it
        }
    }

    private fun getEvaluator(expression: String): CelRuntime.Program? = programCache.computeIfAbsent(expression) { condition ->
        val ast = astCache.computeIfAbsent(condition) {
            logger.debug("Compiling CEL expression: {}", condition)
            celCompiler.compile(it).ast
        }
        celRuntime.createProgram(ast)
    }

}

fun Any?.toCelCompatible(): Any? = when (this) {
    is GenericRecord -> this.schema.fields.associate { it.name() to this.get(it.pos()).toCelCompatible() }
    is CharSequence -> this.toString() // Converts Avro Utf8 to java.lang.String
    is List<*> -> this.map { it.toCelCompatible() }
    is Map<*, *> -> this.entries.associate { it.key.toString() to it.value.toCelCompatible() }
    else -> this
}

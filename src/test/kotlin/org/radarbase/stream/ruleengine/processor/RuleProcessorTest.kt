package org.radarbase.stream.ruleengine.processor

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.processor.api.RecordMetadata
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.any
import org.mockito.Mockito.atLeast
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.radarbase.config.intervention.ActionConfig
import org.radarbase.config.intervention.ConditionConfig
import org.radarbase.config.intervention.ExpressionType
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.ruleengine.domain.RuleGroup
import java.util.Optional
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RuleProcessorTest {

    private lateinit var store: FakeRuleGroupStore
    private lateinit var context: ProcessorContext<GenericRecord, ActionConfig>
    private lateinit var processor: RuleProcessor

    @BeforeEach
    fun setUp() {
        store = FakeRuleGroupStore()

        @Suppress("UNCHECKED_CAST")
        val mockContext = mock(ProcessorContext::class.java) as ProcessorContext<GenericRecord, ActionConfig>
        `when`(mockContext.getStateStore<FakeRuleGroupStore>(any())).thenReturn(store)
        `when`(mockContext.recordMetadata()).thenReturn(
            Optional.of(
                object : RecordMetadata {
                    override fun topic(): String = "input_topic"
                    override fun partition(): Int = 0
                    override fun offset(): Long = 0
                },
            ),
        )
        context = mockContext

        processor = RuleProcessor("store-name")
        processor.init(context)
    }

    @Test
    fun `a global-scoped rule matches regardless of key`() {
        seed(ScopePrefix.GLOBAL.scope, "rule1", interventionConfig("rule1", listOf(condition("value.heartRate > 120.0"))))

        processor.process(Record(observationKey(), sensorValue(130.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `a project-scoped rule matches via the ObservationKey projectId field`() {
        val config = interventionConfig("rule1", listOf(condition("true", projects = listOf("project-1"))))
        seed("project.project-1", "rule1", config)

        processor.process(Record(observationKey(projectId = "project-1"), sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `a user-scoped rule matches via the ObservationKey userId field`() {
        val config = interventionConfig("rule1", listOf(condition("true", subjects = listOf("subject-1"))))
        seed("user.subject-1", "rule1", config)

        processor.process(Record(observationKey(userId = "subject-1"), sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `a rule present in multiple scope buckets fires only once`() {
        val config = interventionConfig(
            "rule1",
            listOf(
                condition("true", name = "global_cond"),
                condition("true", name = "project_cond", projects = listOf("project-1")),
            ),
        )
        seed(ScopePrefix.GLOBAL.scope, "rule1", config)
        seed("project.project-1", "rule1", config)

        processor.process(Record(observationKey(projectId = "project-1"), sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `conditions scoped to a different project are excluded before evaluation`() {
        val config = interventionConfig(
            "rule1",
            listOf(
                condition("true", name = "applicable", projects = listOf("project-1")),
                condition("false", name = "out_of_scope", projects = listOf("project-2")),
            ),
        )
        seed("project.project-1", "rule1", config)

        processor.process(Record(observationKey(projectId = "project-1"), sensorValue(0.0), 0L))

        assertEquals(1, forwardedActions().size)
    }

    @Test
    fun `all applicable conditions must hold for the rule to fire`() {
        val config = interventionConfig(
            "rule1",
            listOf(condition("true", name = "cond_a"), condition("false", name = "cond_b")),
        )
        seed(ScopePrefix.GLOBAL.scope, "rule1", config)

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        assertEquals(0, forwardedActions().size)
    }

    @Test
    fun `one output record is forwarded per matched action`() {
        val config = interventionConfig(
            "rule1",
            listOf(condition("true")),
            actions = listOf(ActionConfig(name = "action-a"), ActionConfig(name = "action-b")),
        )
        seed(ScopePrefix.GLOBAL.scope, "rule1", config)

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        val actionNames = forwardedActions().map { it.value().name }
        assertEquals(listOf("action-a", "action-b"), actionNames)
    }

    @Test
    fun `an action scoped to a different project does not fire outside that scope`() {
        val config = interventionConfig(
            "rule1",
            listOf(condition("true")),
            actions = listOf(ActionConfig(name = "action-a", projects = listOf("other-project"))),
        )
        seed(ScopePrefix.GLOBAL.scope, "rule1", config)

        processor.process(Record(observationKey(projectId = "project-1"), sensorValue(0.0), 0L))

        assertTrue(forwardedActions().isEmpty())
    }

    @Test
    fun `a malformed expression in one rule does not block other rules from firing`() {
        val badConfig = interventionConfig(
            "bad_rule",
            listOf(condition("value.. invalid((", name = "bad_cond")),
            actions = listOf(ActionConfig(name = "bad_action")),
        )
        val goodConfig = interventionConfig(
            "good_rule",
            listOf(condition("true", name = "good_cond")),
            actions = listOf(ActionConfig(name = "good_action")),
        )
        seed(ScopePrefix.GLOBAL.scope, "bad_rule", badConfig)
        seed(ScopePrefix.GLOBAL.scope, "good_rule", goodConfig)

        processor.process(Record(observationKey(), sensorValue(0.0), 0L))

        val actionNames = forwardedActions().map { it.value().name }
        assertEquals(listOf("good_action"), actionNames)
    }

    private fun seed(scopeKey: String, storeKey: String, config: InterventionConfig) {
        val rules = store.get(scopeKey)?.rules ?: mutableMapOf()
        rules[storeKey] = config
        store.putDirect(scopeKey, RuleGroup(rules))
    }

    @Suppress("UNCHECKED_CAST")
    private fun forwardedActions(): List<Record<GenericRecord, ActionConfig>> {
        val captor = ArgumentCaptor.forClass(Record::class.java) as ArgumentCaptor<Record<GenericRecord, ActionConfig>>
        verify(context, atLeast(0)).forward(captor.capture())
        return captor.allValues
    }

    private fun interventionConfig(
        name: String,
        conditions: List<ConditionConfig>,
        actions: List<ActionConfig> = listOf(ActionConfig(name = "notify")),
    ) = InterventionConfig(name = name, topic = "output_topic", conditionConfigs = conditions, actionConfigs = actions)

    private fun condition(
        expression: String,
        name: String = "cond",
        projects: List<String>? = null,
        subjects: List<String>? = null,
    ) = ConditionConfig(type = ExpressionType.CEL, expression = expression, name = name, projects = projects, subjects = subjects)

    private fun observationKey(projectId: String? = null, userId: String? = null, sourceId: String? = null): GenericRecord =
        GenericData.Record(KEY_SCHEMA).apply {
            put("projectId", projectId)
            put("userId", userId)
            put("sourceId", sourceId)
        }

    private fun sensorValue(heartRate: Double): GenericRecord =
        GenericData.Record(VALUE_SCHEMA).apply {
            put("heartRate", heartRate)
        }

    companion object {
        private val KEY_SCHEMA: Schema = Schema.Parser().parse(
            """
            {
              "type": "record",
              "name": "ObservationKey",
              "namespace": "org.radarbase.stream.ruleengine.test",
              "fields": [
                {"name": "projectId", "type": ["null", "string"], "default": null},
                {"name": "userId", "type": ["null", "string"], "default": null},
                {"name": "sourceId", "type": ["null", "string"], "default": null}
              ]
            }
            """.trimIndent(),
        )

        private val VALUE_SCHEMA: Schema = Schema.Parser().parse(
            """
            {
              "type": "record",
              "name": "TestSensorValue",
              "namespace": "org.radarbase.stream.ruleengine.test",
              "fields": [
                {"name": "heartRate", "type": "double"}
              ]
            }
            """.trimIndent(),
        )
    }
}

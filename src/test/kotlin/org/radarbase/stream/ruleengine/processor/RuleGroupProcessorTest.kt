package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.radarbase.config.intervention.ActionConfig
import org.radarbase.config.intervention.ConditionConfig
import org.radarbase.config.intervention.ExpressionType
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.ruleengine.domain.RuleKey
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RuleGroupProcessorTest {

    private lateinit var store: FakeRuleGroupStore
    private lateinit var processor: RuleGroupProcessor

    @BeforeEach
    fun setUp() {
        store = FakeRuleGroupStore()

        @Suppress("UNCHECKED_CAST")
        val context = mock(ProcessorContext::class.java) as ProcessorContext<Void, Void>
        `when`(context.getStateStore<FakeRuleGroupStore>(any())).thenReturn(store)

        processor = RuleGroupProcessor("store-name")
        processor.init(context)
    }

    @Test
    fun `condition with subjects maps to a user scope`() {
        val config = interventionConfig(name = "rule1", subjects = listOf("subject-1"))
        processor.process(Record(ruleKey("rule1"), config, 0L))

        assertEquals(setOf(ruleKey("rule1").toStoreKey()), store.get("user.subject-1")!!.rules.keys)
        assertNull(store.get("project.subject-1"))
    }

    @Test
    fun `condition with projects maps to a project scope`() {
        val config = interventionConfig(name = "rule1", projects = listOf("project-1"))
        processor.process(Record(ruleKey("rule1"), config, 0L))

        assertTrue(store.get("project.project-1")!!.rules.containsKey(ruleKey("rule1").toStoreKey()))
        assertNull(store.get("user.project-1"))
    }

    @Test
    fun `condition with neither subjects nor projects maps to the global scope`() {
        val config = interventionConfig(name = "rule1")
        processor.process(Record(ruleKey("rule1"), config, 0L))

        assertTrue(store.get(ScopePrefix.GLOBAL.scope)!!.rules.containsKey(ruleKey("rule1").toStoreKey()))
    }

    @Test
    fun `moving a rule from one scope to another removes it from the old scope`() {
        val key = ruleKey("rule1")
        val originalConfig = interventionConfig(name = "rule1", projects = listOf("project-1"))
        processor.process(Record(key, originalConfig, 0L))
        assertTrue(store.get("project.project-1")!!.rules.containsKey(key.toStoreKey()))

        val movedConfig = interventionConfig(name = "rule1", subjects = listOf("subject-1"))
        processor.process(Record(key, movedConfig, 0L))

        assertNull(store.get("project.project-1")?.rules?.get(key.toStoreKey()))
        assertTrue(store.get("user.subject-1")!!.rules.containsKey(key.toStoreKey()))
    }

    @Test
    fun `a tombstone removes the rule from all scopes it was in`() {
        val key = ruleKey("rule1")
        val config = interventionConfig(name = "rule1", projects = listOf("project-1"))
        processor.process(Record(key, config, 0L))
        assertTrue(store.get("project.project-1")!!.rules.containsKey(key.toStoreKey()))

        processor.process(Record(key, null, 0L))

        assertNull(store.get("project.project-1"))
    }

    @Test
    fun `multiple rules aggregate into the same scope`() {
        val configA = interventionConfig(name = "rule_a", projects = listOf("project-1"))
        val configB = interventionConfig(name = "rule_b", projects = listOf("project-1"))

        processor.process(Record(ruleKey("rule_a"), configA, 0L))
        processor.process(Record(ruleKey("rule_b"), configB, 0L))

        assertEquals(
            setOf(ruleKey("rule_a").toStoreKey(), ruleKey("rule_b").toStoreKey()),
            store.get("project.project-1")!!.rules.keys,
        )
    }

    private fun ruleKey(name: String) = RuleKey(clientId = "client", scope = "config", name = name)

    private fun interventionConfig(
        name: String,
        projects: List<String>? = null,
        subjects: List<String>? = null,
    ) = InterventionConfig(
        name = name,
        topic = "output_topic",
        conditionConfigs = listOf(
            ConditionConfig(
                type = ExpressionType.CEL,
                expression = "true",
                name = "${name}_condition",
                projects = projects,
                subjects = subjects,
            ),
        ),
        actionConfigs = listOf(ActionConfig(name = "notify")),
    )
}

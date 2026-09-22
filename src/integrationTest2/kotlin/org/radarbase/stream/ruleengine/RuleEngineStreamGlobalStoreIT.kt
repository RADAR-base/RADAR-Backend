package org.radarbase.stream.ruleengine

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.radarbase.config.SingleStreamConfig
import org.radarbase.config.intervention.ActionConfig
import org.radarbase.config.intervention.ConditionConfig
import org.radarbase.config.intervention.ExpressionType
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.StreamMaster
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.harness.FixtureRadarConfigHandler
import org.radarbase.stream.ruleengine.harness.KafkaBroker
import org.radarbase.stream.ruleengine.harness.KafkaBrokerExtension
import org.radarbase.stream.ruleengine.harness.KafkaTopics
import org.radarbase.stream.ruleengine.harness.RuleEngineConfigFixtures
import org.radarbase.stream.ruleengine.harness.StateStoreQueries
import org.radarbase.stream.ruleengine.harness.StreamReadiness
import org.radarbase.stream.ruleengine.processor.ScopePrefix
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.stream.Stream

@ExtendWith(KafkaBrokerExtension::class)
class RuleEngineStreamGlobalStoreIT {

    private val workers = mutableListOf<RuleEngineStream>()

    @AfterEach
    fun tearDown() {
        // RuleEngineStream.doCleanup() is an unimplemented TODO() (open question in the harness
        // plan), so worker.shutdown() would throw here. Close the underlying KafkaStreams
        // instances directly instead of going through shutdown()/closeStreams().
        workers.forEach { worker -> worker.kafkaStreamsInstances?.forEach { it.close(Duration.ofSeconds(10)) } }
    }

    @Test
    fun `global store is populated from a global-scope rule`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(clientId = "radar-backend-it", scope = "config", name = "high_heart_rate")
        val config = interventionConfig(name = "high_heart_rate", topic = topics.outputTopic, expression = "value.heartRate > 120")

        publish(topics, id = 1, ruleKey = ruleKey, config = config)

        val streams = startWorker(topics, stateDir)
        StreamReadiness.awaitRunning(listOf(streams))

        val ruleGroup: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, ScopePrefix.GLOBAL.scope)

        assertNotNull(ruleGroup, "Expected the global-scope rule to land in the store under the '${ScopePrefix.GLOBAL.scope}' key")
        assertEquals(setOf(ruleKey.toStoreKey()), ruleGroup!!.rules.keys)
    }

    @Test
    fun `global store aggregates multiple rules for the same scope`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKeyA = RuleKey(clientId = "radar-backend-it", scope = "project.test-questionnaire", name = "rule_a")
        val ruleKeyB = RuleKey(clientId = "radar-backend-it", scope = "project.test-questionnaire", name = "rule_b")
        val configA = interventionConfig(name = "rule_a", topic = topics.outputTopic, expression = "value.heartRate > 100")
        val configB = interventionConfig(name = "rule_b", topic = topics.outputTopic, expression = "value.heartRate > 150")

        // Both conditions have no projects/subjects set, so RuleGroupProcessor.toScopeKeys()
        // routes them both to the same "global" store scope key - reproducing the real-data shape
        // (multiple rule names under one scope) that previously broke the RuleGroup/RuleKey map
        // round-trip.
        publish(topics, id = 1, ruleKey = ruleKeyA, config = configA)
        publish(topics, id = 2, ruleKey = ruleKeyB, config = configB)

        val streams = startWorker(topics, stateDir)
        StreamReadiness.awaitRunning(listOf(streams))

        val ruleGroup: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, ScopePrefix.GLOBAL.scope)

        assertNotNull(ruleGroup, "Expected both rules to land in the store under the '${ScopePrefix.GLOBAL.scope}' key")
        assertEquals(setOf(ruleKeyA.toStoreKey(), ruleKeyB.toStoreKey()), ruleGroup!!.rules.keys)
    }

    @Test
    fun `global store restore survives malformed legacy JSON and keeps the later well-formed record`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val ruleKey = RuleKey(clientId = "radar-backend-it", scope = "project.test-questionnaire", name = "rule4")
        val config = interventionConfig(name = "rule4", topic = topics.outputTopic, expression = "value.heartRate > 120")

        // Reproduces a real historical row from rule_engine_config (scope=project.test-questionnaire,
        // name=rule4, version=1): missing comma between the "expression" and "projects" fields.
        val malformedJson = """
            {"name":"rule4","topic":"${topics.outputTopic}","conditions":[{"type":"cel","expression":"value.heartRate > 120"       "projects":["STAGING_PROJECT"],"subjects":null,"name":"rule4_condition"}],"actions":[{"name":"notify"}]}
        """.trimIndent()

        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.inputTopic, topics.outputTopic, topics.globalStoreTopic)
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey.clientId, ruleKey.scope, ruleKey.name),
            RuleEngineConfigFixtures.valueRaw(id = 1, clientId = ruleKey.clientId, scope = ruleKey.scope, name = ruleKey.name, rawJson = malformedJson),
        )
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey.clientId, ruleKey.scope, ruleKey.name),
            RuleEngineConfigFixtures.value(id = 2, clientId = ruleKey.clientId, scope = ruleKey.scope, name = ruleKey.name, config = config),
        )

        val streams = startWorker(topics, stateDir)

        // The stream must reach RUNNING despite the malformed row - previously the default
        // LogAndFailExceptionHandler killed the global stream thread on this exact shape.
        StreamReadiness.awaitRunning(listOf(streams))

        val ruleGroup: RuleGroup? = StateStoreQueries.waitForValue(streams, topics.globalStoreName, ScopePrefix.GLOBAL.scope)

        assertNotNull(ruleGroup, "Expected the later well-formed record to land in the store under the '${ScopePrefix.GLOBAL.scope}' key")
        assertEquals(config, ruleGroup!!.rules[ruleKey.toStoreKey()])
    }

    private fun publish(topics: Topics, id: Int, ruleKey: RuleKey, config: InterventionConfig) {
        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.inputTopic, topics.outputTopic, topics.globalStoreTopic)
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey.clientId, ruleKey.scope, ruleKey.name),
            RuleEngineConfigFixtures.value(id = id, clientId = ruleKey.clientId, scope = ruleKey.scope, name = ruleKey.name, config = config),
        )
    }

    private fun interventionConfig(name: String, topic: String, expression: String) = InterventionConfig(
        name = name,
        topic = topic,
        conditionConfigs = listOf(
            ConditionConfig(type = ExpressionType.CEL, expression = expression, name = "${name}_condition"),
        ),
        actionConfigs = listOf(ActionConfig(name = "notify")),
    )

    private fun startWorker(topics: Topics, stateDir: Path): KafkaStreams {
        val singleStreamConfig = SingleStreamConfig().apply {
            streamClass = RuleEngineStream::class.java
            properties = mapOf(
                "input_topic" to topics.inputTopic,
                "output_topic" to topics.outputTopic,
                "global_store_name" to topics.globalStoreName,
                "global_store_topic" to topics.globalStoreTopic,
                StreamsConfig.STATE_DIR_CONFIG to stateDir.toString(),
            )
        }

        val configHandler = FixtureRadarConfigHandler(KafkaBroker.bootstrapServers)
        val master = StreamMaster(configHandler, Stream.empty())
        val worker = RuleEngineStream()
        worker.configure(master, configHandler, singleStreamConfig)
        worker.start()
        workers += worker

        return requireNotNull(worker.kafkaStreamsInstances) { "RuleEngineStream did not build any KafkaStreams instances" }.first()
    }

    private data class Topics(
        val inputTopic: String,
        val outputTopic: String,
        val globalStoreTopic: String,
        val globalStoreName: String,
    ) {
        companion object {
            fun unique(): Topics {
                val suffix = UUID.randomUUID().toString().replace("-", "")
                return Topics(
                    inputTopic = "input_topic_$suffix",
                    outputTopic = "output_topic_$suffix",
                    globalStoreTopic = "rule_engine_config_$suffix",
                    globalStoreName = "JIT_RULES_STORE_$suffix",
                )
            }
        }
    }
}

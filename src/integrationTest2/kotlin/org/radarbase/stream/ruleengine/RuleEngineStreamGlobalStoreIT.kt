package org.radarbase.stream.ruleengine

import org.apache.kafka.streams.StreamsConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.io.TempDir
import org.radarbase.config.SingleStreamConfig
import org.radarbase.config.intervention.ActionConfig
import org.radarbase.config.intervention.ConditionConfig
import org.radarbase.config.intervention.ExpressionType
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.StreamMaster
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.harness.FixtureRadarConfigHandler
import org.radarbase.stream.ruleengine.harness.KafkaBroker
import org.radarbase.stream.ruleengine.harness.KafkaBrokerExtension
import org.radarbase.stream.ruleengine.harness.KafkaTopics
import org.radarbase.stream.ruleengine.harness.RuleEngineConfigFixtures
import org.radarbase.stream.ruleengine.harness.StreamReadiness
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.stream.Stream

@ExtendWith(KafkaBrokerExtension::class)
class RuleEngineStreamGlobalStoreIT {

    private lateinit var worker: RuleEngineStream

    @AfterEach
    fun tearDown() {
        // RuleEngineStream.doCleanup() is an unimplemented TODO() (open question in the harness
        // plan), so worker.shutdown() would throw here. Close the underlying KafkaStreams
        // instances directly instead of going through shutdown()/closeStreams().
        if (::worker.isInitialized) {
            worker.kafkaStreamsInstances?.forEach { it.close(Duration.ofSeconds(10)) }
        }
    }

    @Test
    fun `global store is populated from a global-scope rule`(@TempDir stateDir: Path) {
        val bootstrapServers = KafkaBroker.bootstrapServers
        val suffix = UUID.randomUUID().toString().replace("-", "")
        val inputTopic = "input_topic_$suffix"
        val outputTopic = "output_topic_$suffix"
        val globalStoreTopic = "rule_engine_config_$suffix"
        val globalStoreName = "JIT_RULES_STORE_$suffix"

        KafkaTopics.createTopics(bootstrapServers, inputTopic, outputTopic, globalStoreTopic)

        val ruleKey = RuleKey(clientId = "radar-backend-it", scope = "config", name = "high_heart_rate")
        val interventionConfig = InterventionConfig(
            name = "high_heart_rate",
            topic = outputTopic,
            conditionConfigs = listOf(
                ConditionConfig(
                    type = ExpressionType.CEL,
                    expression = "value.heartRate > 120",
                    name = "high_heart_rate_condition",
                ),
            ),
            actionConfigs = listOf(ActionConfig(name = "notify")),
        )
        RuleEngineConfigFixtures.publish(
            bootstrapServers,
            globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey.clientId, ruleKey.scope, ruleKey.name),
            RuleEngineConfigFixtures.value(
                id = 1,
                clientId = ruleKey.clientId,
                scope = ruleKey.scope,
                name = ruleKey.name,
                config = interventionConfig,
            ),
        )

        val singleStreamConfig = SingleStreamConfig().apply {
            streamClass = RuleEngineStream::class.java
            properties = mapOf(
                "input_topic" to inputTopic,
                "output_topic" to outputTopic,
                "global_store_name" to globalStoreName,
                "global_store_topic" to globalStoreTopic,
                StreamsConfig.STATE_DIR_CONFIG to stateDir.toString(),
            )
        }

        val configHandler = FixtureRadarConfigHandler(bootstrapServers)
        val master = StreamMaster(configHandler, Stream.empty())
        worker = RuleEngineStream()
        worker.configure(master, configHandler, singleStreamConfig)
        worker.start()

        val streamsInstances = requireNotNull(worker.kafkaStreamsInstances) { "RuleEngineStream did not build any KafkaStreams instances" }
        StreamReadiness.awaitRunning(streamsInstances)
    }
}

package org.radarbase.stream.ruleengine

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
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
import org.radarbase.stream.ruleengine.harness.OutputTopicConsumer
import org.radarbase.stream.ruleengine.harness.RuleEngineConfigFixtures
import org.radarbase.stream.ruleengine.harness.StreamReadiness
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.pebble.Pebble2HeartRate
import java.nio.file.Path
import java.time.Duration
import java.util.Properties
import java.util.UUID

/**
 * Exercises the full `input_topic` -> `RuleProcessor` -> `output_topic` path, unlike
 * [RuleEngineStreamGlobalStoreIT] which only asserts on the global store. The `input_topic` side
 * decodes real Avro `GenericRecord`s via the default `SpecificAvroSerde`, which needs a working
 * schema registry; [FixtureRadarConfigHandler] is pointed at a `mock://<scope>` URL backed by
 * Confluent's in-memory `MockSchemaRegistryClient` rather than a real Schema Registry
 * testcontainer. `SpecificAvroSerde`'s deserializer always resolves the writer's schema to a
 * generated `SpecificRecord` class by name, so the records below use the real
 * `org.radarcns.kafka.ObservationKey` / `org.radarcns.passive.pebble.Pebble2HeartRate` classes
 * (already on the classpath via `radar-schemas-commons`) rather than an ad-hoc test schema.
 */
@ExtendWith(KafkaBrokerExtension::class)
class RuleEngineActionForwardingIT {

    private val workers = mutableListOf<RuleEngineStream>()
    private val mapper = jacksonObjectMapper()

    @AfterEach
    fun tearDown() {
        workers.forEach { worker -> worker.kafkaStreamsInstances?.forEach { it.close(Duration.ofSeconds(1)) } }
    }

    @Test
    fun `a matching sensor record forwards the matched action to the output topic`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val schemaRegistryUrl = "mock://${topics.globalStoreName}"

        val ruleKey = RuleKey(clientId = "radar-backend-it", scope = "config", name = "high_heart_rate")
        val config = interventionConfig(name = "high_heart_rate", expression = "value.time > 120.0")

        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.inputTopic, topics.outputTopic, topics.globalStoreTopic)
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey.clientId, ruleKey.scope, ruleKey.name),
            RuleEngineConfigFixtures.value(id = 1, clientId = ruleKey.clientId, scope = ruleKey.scope, name = ruleKey.name, config = config),
        )

        val streams = startWorker(topics, stateDir, schemaRegistryUrl)
        StreamReadiness.awaitRunning(listOf(streams), Duration.ofSeconds(1))

        publishSensorRecord(topics.inputTopic, schemaRegistryUrl, projectId = "project-1", time = 130.0)
        publishSensorRecord(topics.inputTopic, schemaRegistryUrl, projectId = "project-1", time = 90.0)

        val actions = OutputTopicConsumer.waitForRecordValues(
            bootstrapServers = KafkaBroker.bootstrapServers,
            topic = topics.outputTopic,
            minCount = 1,
            timeout = Duration.ofSeconds(1),
        ) { bytes -> mapper.readValue(bytes, ActionConfig::class.java) }

        assertEquals(listOf(ActionConfig(name = "notify")), actions)
    }

    @Test
    fun `a non-matching sensor record produces nothing on the output topic`(@TempDir stateDir: Path) {
        val topics = Topics.unique()
        val schemaRegistryUrl = "mock://${topics.globalStoreName}"

        val ruleKey = RuleKey(clientId = "radar-backend-it", scope = "config", name = "high_heart_rate")
        val config = interventionConfig(name = "high_heart_rate", expression = "value.time > 120.0")

        KafkaTopics.createTopics(KafkaBroker.bootstrapServers, topics.inputTopic, topics.outputTopic, topics.globalStoreTopic)
        RuleEngineConfigFixtures.publish(
            KafkaBroker.bootstrapServers,
            topics.globalStoreTopic,
            RuleEngineConfigFixtures.key(ruleKey.clientId, ruleKey.scope, ruleKey.name),
            RuleEngineConfigFixtures.value(id = 1, clientId = ruleKey.clientId, scope = ruleKey.scope, name = ruleKey.name, config = config),
        )

        val streams = startWorker(topics, stateDir, schemaRegistryUrl)
        StreamReadiness.awaitRunning(listOf(streams), Duration.ofSeconds(1))

        publishSensorRecord(topics.inputTopic, schemaRegistryUrl, projectId = "project-1", time = 90.0)

        val actions = OutputTopicConsumer.waitForRecordValues(
            bootstrapServers = KafkaBroker.bootstrapServers,
            topic = topics.outputTopic,
            minCount = 1,
            timeout = Duration.ofSeconds(1),
        ) { bytes -> mapper.readValue(bytes, ActionConfig::class.java) }

        assertTrue(actions.isEmpty(), "Expected no actions to be forwarded for a non-matching record, got $actions")
    }

    private fun publishSensorRecord(topic: String, schemaRegistryUrl: String, projectId: String, time: Double) {
        val serializerConfig = mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl)
        val keySerializer = KafkaAvroSerializer().apply { configure(serializerConfig, true) }
        val valueSerializer = KafkaAvroSerializer().apply { configure(serializerConfig, false) }

        val key = ObservationKey(projectId, "user-1", "source-1")
        val value = Pebble2HeartRate(time, time, 60.0f)

        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBroker.bootstrapServers)
        }
        KafkaProducer(props, keySerializer, valueSerializer).use { producer ->
            producer.send(ProducerRecord(topic, key, value)).get()
        }
    }

    private fun interventionConfig(
        name: String,
        expression: String,
        projects: List<String>? = null,
        subjects: List<String>? = null,
    ) = InterventionConfig(
        name = name,
        topic = "output_topic",
        conditionConfigs = listOf(
            ConditionConfig(
                type = ExpressionType.CEL,
                expression = expression,
                name = "${name}_condition",
                projects = projects,
                subjects = subjects,
            ),
        ),
        actionConfigs = listOf(ActionConfig(name = "notify")),
    )

    private fun startWorker(topics: Topics, stateDir: Path, schemaRegistryUrl: String): KafkaStreams {
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

        val configHandler = FixtureRadarConfigHandler(KafkaBroker.bootstrapServers, schemaRegistryUrl)
        val master = StreamMaster(configHandler, java.util.stream.Stream.empty())
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

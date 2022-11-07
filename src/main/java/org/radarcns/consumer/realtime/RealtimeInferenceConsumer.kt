package org.radarcns.consumer.realtime

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.errors.WakeupException
import org.radarbase.util.RollingTimeAverage
import org.radarcns.config.ConfigRadar
import org.radarcns.config.realtime.ActionConfig
import org.radarcns.config.realtime.ConditionConfig
import org.radarcns.config.realtime.RealtimeConsumerConfig
import org.radarcns.consumer.realtime.RealtimeInferenceConsumer
import org.radarcns.consumer.realtime.action.Action
import org.radarcns.consumer.realtime.action.ActionFactory.getActionFor
import org.radarcns.consumer.realtime.condition.Condition
import org.radarcns.consumer.realtime.condition.ConditionFactory.getConditionFor
import org.radarcns.monitor.KafkaMonitor
import org.radarcns.util.EmailSender
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.*

/**
 * The main Kafka Consumer class that runs a single consumer on any topic. The consumer evaluates
 * each incoming record based on the [Condition]s provided in the config. If and only If all
 * the conditions evaluate to true, only then all the configured [Action]s are fired.
 *
 *
 * To be used with the model-invocation-endpoint and KSQL API_INFERENCE function to evaluate and
 * take action on incoming results from realtime inference on data.
 */
class RealtimeInferenceConsumer(
        groupId: String?, clientId: String, radar: ConfigRadar, consumerConfig: RealtimeConsumerConfig) : KafkaMonitor {
    private val properties: Properties = Properties().apply {
        setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "${consumerConfig.name}-$clientId")
        setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        val deserializer = KafkaAvroDeserializer::class.java.name
        setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
        setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
        setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1001")
        setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15101")
        setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "7500")
        setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, radar.schemaRegistryPaths)
        setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, radar.brokerPaths)
        // Override with any properties specified for this consumer
        consumerConfig.consumerProperties?.let { putAll(it) }
    }
    private val conditions: List<Condition> = consumerConfig.conditionConfigs
            .map { c: ConditionConfig ->
                getConditionFor(c)
            }
    private val actions: List<Action> = consumerConfig.actionConfigs
            .map { a: ActionConfig ->
                getActionFor(radar, a)
            }

    private val topic: String = consumerConfig.topic
    private var done: Boolean = false
    private var pollTimeout: Duration = Duration.ofDays(365)
    private var consumer: Consumer<GenericRecord, GenericRecord>? = null
    private val notifyErrorEmailSender: EmailSender?

    init {
        require(topic.isNotBlank()) { "Cannot start consumer without topic." }
        require(!(conditions.isEmpty() || actions.isEmpty())) {
            "At least one each of condition and action is necessary to run the consumer."
        }

        notifyErrorEmailSender = consumerConfig.notifyErrorsEmails?.emailAddresses?.let {
            EmailSender(radar.emailServerConfig, radar.emailServerConfig.user, it)
        }
    }

    @Throws(IOException::class, InterruptedException::class)
    override fun start() {
        consumer = KafkaConsumer(properties)
        consumer?.subscribe(setOf(topic))
        logger.info("Consuming realtime inference topic {}", topic)
        val ops = RollingTimeAverage(20000)
        try {
            while (!isShutdown) {
                try {
                    val records = consumer?.poll(getPollTimeout()) ?: continue
                    ops.add(records.count().toDouble())
                    for (record in records) {
                        if (conditions.evaluateAll(record)) {
                            // Only execute the actions if all the conditions are true
                            actions.executeAll(record)
                        }
                    }
                } catch (ex: SerializationException) {
                    logger.warn("Failed to deserialize the record: {}", ex.message)
                    notifyErrorEmailSender?.notifyErrors(null, ex)
                } catch (ex: WakeupException) {
                    logger.info("Consumer woke up")
                    notifyErrorEmailSender?.notifyErrors(null, ex)
                } catch (ex: InterruptException) {
                    logger.info("Consumer was interrupted")
                    notifyErrorEmailSender?.notifyErrors(null, ex)
                    shutdown()
                } catch (ex: KafkaException) {
                    logger.error("Kafka consumer gave exception", ex)
                    notifyErrorEmailSender?.notifyErrors(null, ex)
                } catch (ex: Exception) {
                    logger.error("Consumer gave exception", ex)
                    notifyErrorEmailSender?.notifyErrors(null, ex)
                }
            }
        } finally {
            consumer?.close()
        }
    }

    private fun List<Condition>.evaluateAll(record: ConsumerRecord<GenericRecord, GenericRecord>): Boolean {
        return this.all { c: Condition ->
            try {
                c.evaluate(record)
            } catch (exc: IOException) {
                logger.warn(
                        "I/O Error evaluating one of the conditions: {}. Will not continue.",
                        c.name,
                        exc)
                notifyErrorEmailSender?.notifyErrors(record, exc, condition = c)
                false
            } catch (exc: Exception) {
                logger.warn(
                        "Error evaluating one of the conditions: {}. Will not continue.",
                        c.name,
                        exc)
                notifyErrorEmailSender?.notifyErrors(record, exc, condition = c)
                false
            }
        }
    }

    private fun List<Action>.executeAll(record: ConsumerRecord<GenericRecord, GenericRecord>): List<Boolean> {
        return this.map { a: Action ->
            try {
                a.run(record)
            } catch (ex: IllegalArgumentException) {
                logger.warn("Argument was not valid. Error executing action", ex)
                notifyErrorEmailSender?.notifyErrors(record, ex, a)
                false
            } catch (ex: IOException) {
                logger.warn("I/O Error executing action", ex)
                notifyErrorEmailSender?.notifyErrors(record, ex, action = a)
                false
            } catch (ex: Exception) {
                // Catch all exceptions so that we can continue to execute the other actions
                logger.warn("Error executing action", ex)
                notifyErrorEmailSender?.notifyErrors(record, ex, action = a)
                false
            }
        }
    }

    @Throws(IOException::class, InterruptedException::class)
    override fun shutdown() {
        logger.info("Shutting down consumer {}", javaClass.simpleName)
        done = true
        consumer?.wakeup()
    }

    override fun isShutdown(): Boolean {
        return done
    }

    override fun getPollTimeout(): Duration {
        return pollTimeout
    }

    override fun setPollTimeout(duration: Duration) {
        pollTimeout = duration
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RealtimeInferenceConsumer::class.java)

        private fun EmailSender.notifyErrors(
                record: ConsumerRecord<GenericRecord, GenericRecord>?,
                throwable: Throwable,
                action: Action? = null,
                condition: Condition? = null,
        ) {
            val prefix = when {
                action != null -> "Action ${action.name} failed"
                condition != null -> "Condition ${condition.name} failed"
                else -> "Unknown error"
            }

            val customSubject = "$prefix for ${record?.key()?.toString() ?: "Realtime Inference"}"

            val customBody = "$prefix for ${record?.key()?.toString()}:\n" +
                    "Error: ${throwable.message}\n\n" +
                    "Stacktrace: \n${throwable.stackTrace.joinToString("\n\t")}"

            sendEmail(customSubject, customBody)
        }
    }
}
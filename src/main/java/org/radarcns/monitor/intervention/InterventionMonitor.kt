package org.radarcns.monitor.intervention

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.kafka.serializers.KafkaJsonDeserializer
import okhttp3.OkHttpClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.config.RadarPropertyHandler
import org.radarcns.config.monitor.InterventionMonitorConfig
import org.radarcns.monitor.AbstractKafkaMonitor
import org.radarcns.monitor.intervention.InterventionRecord.Companion.toInterventionRecord
import org.radarbase.appserver.client.protocol.FileProtocolDirectory
import org.radarcns.util.EmailSenders
import org.slf4j.LoggerFactory
import java.time.*
import java.util.*
import java.util.concurrent.*

/**
 * The main Kafka Consumer class that runs a single consumer on any topic. The consumer evaluates
 * each incoming record based on the Conditions provided in the config. If and only If all
 * the conditions evaluate to true, only then all the configured Actions are fired.
 *
 * To be used with the model-invocation-endpoint and KSQL API_INFERENCE function to evaluate and
 * take action on incoming results from realtime inference on data.
 */
class InterventionMonitor(
    config: InterventionMonitorConfig,
    radar: RadarPropertyHandler,
    emailSenders: EmailSenders?,
) : AbstractKafkaMonitor<String, Map<String, Any>, InterventionMonitorState>(
    radar,
    listOf(config.topic),
    "intervention_monitors",
    "1",
    InterventionMonitorState(),
) {
    private val queue: MutableMap<String, Future<*>> = HashMap()
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val deadline: Duration = config.deadline
    private val ttlMargin: Duration = config.ttlMargin
    private val maxInterventions = config.maxInterventions
    private val thresholdAdjust: ThresholdAdjustmentAlgorithm
    private val appServerNotifications: AppServerIntervention
    private var emailer: InterventionExceptionEmailer?
    private val exceptionBatch: LinkedBlockingQueue<InterventionRecord> = LinkedBlockingQueue()

    init {
        configureConsumer()

        val httpClient = OkHttpClient()
        val mapper = jacksonObjectMapper()

        appServerNotifications = AppServerIntervention(
            protocolDirectory = FileProtocolDirectory(config.protocolDirectory, mapper),
            defaultLanguage = config.defaultLanguage,
            appserverUrl = config.appServerUrl,
            authConfig = config.authConfig,
            httpClient = httpClient,
            mapper = mapper,
        )

        thresholdAdjust = ThresholdAdjustmentAlgorithm(
            clientId = config.ksqlAppConfigClient,
            state = state,
            config = config.thresholdAdaptation,
            appConfigUrl = config.appConfigUrl,
            authConfig = config.authConfig,
            httpClient = httpClient,
            mapper = mapper,
        )

        emailer = if (emailSenders != null) InterventionExceptionEmailer(
            emailSenders = emailSenders,
            state = state,
        ) else null
    }

    private fun configureConsumer() {
        val properties = Properties()
        val deserializer: String = KafkaJsonDeserializer::class.java.name
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
        configure(properties)
    }

    override fun start() {
        val lastMidnight = InterventionMonitorState.lastMidnight()
        if (lastMidnight != state.fromDate) {
            state.reset(lastMidnight)
        }

        super.start()

        // At midnight, update the threshold levels
        val nextMidnight = state.nextMidnight()
        executor.scheduleAtFixedRate(
            {
                try {
                    thresholdAdjust.updateThresholds()
                    emailer?.emailExceptions()
                    resetQueue()
                    state.reset(state.nextMidnight())
                    storeState()
                } catch (ex: Throwable) {
                    logger.error("Failed to update thresholds", ex)
                }
            },
            Duration.between(Instant.now(), nextMidnight).toMillis(),
            Duration.ofHours(24).toMillis(),
            TimeUnit.MILLISECONDS,
        )
    }

    override fun evaluateRecord(record: ConsumerRecord<String, Map<String, Any>>) {
        val intervention = parseRecord(record) ?: return

        if (intervention.exception.isNotEmpty()) {
            addException(intervention)
        }

        val interventionDeadline = intervention.timeCompleted + deadline

        executor.execute {
            val userInterventions = state[intervention].interventions

            if (intervention.decision) {
                userInterventions += intervention.time
                if (userInterventions.size > maxInterventions) {
                    return@execute
                }
            } else {
                userInterventions -= intervention.time
            }

            queue.remove(intervention.queueKey)
                ?.cancel(false)

            val durationBeforeDeadline = Duration.between(Instant.now(), interventionDeadline)
            if (durationBeforeDeadline.isNegative) {
                return@execute
            }
            if (intervention.decision) {
                if (intervention.isFinal) {
                    sendNotification(intervention, interventionDeadline)
                } else {
                    queue[intervention.queueKey] = executor.schedule(
                        {
                            sendNotification(intervention, interventionDeadline,)
                            queue -= intervention.queueKey
                        },
                        durationBeforeDeadline.toMillis(),
                        TimeUnit.MILLISECONDS
                    )
                }
            }
        }
    }

    private fun addException(intervention: InterventionRecord) {
        logger.warn("Record has exception for {} - {}: {}",
            intervention.projectId, intervention.userId, intervention.exception)
        exceptionBatch += intervention
    }

    override fun afterEvaluate() {
        executor.execute {
            val localExceptions = mutableListOf<InterventionRecord>()
            exceptionBatch.drainTo(localExceptions)
            localExceptions.forEach { state.addException(it) }
            super.afterEvaluate()
        }
    }

    private fun parseRecord(record: ConsumerRecord<String, Map<String, Any>>): InterventionRecord? {
        val userId = record.key()
        if (userId.isNullOrEmpty()) {
            logger.error("Cannot map record without user ID")
            return null
        }
        val intervention = try {
            record.value().toInterventionRecord(userId)
        } catch (ex: Exception) {
            logger.error("Cannot map intervention record from {}: {}", record.value(), ex.toString())
            return null
        }
        if (intervention.timeCompleted < state.fromDate) {
            return null
        }
        return intervention
    }

    private fun sendNotification(
        intervention: InterventionRecord,
        interventionDeadline: Instant
    ) {
        state[intervention].numberOfInterventions += 1

        val durationBeforeDeadline = Duration.between(Instant.now(), interventionDeadline)
        val ttl = ttlMargin + durationBeforeDeadline

        appServerNotifications.sendNotification(intervention, ttl)
    }

    private fun resetQueue() {
        queue.values.forEach { it.cancel(false) }
        queue.clear()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(InterventionMonitor::class.java)

        private val InterventionRecord.queueKey: String
            get() = "$userId-$timeCompleted"
    }
}

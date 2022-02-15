package org.radarcns.monitor.intervention

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
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
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * The main Kafka Consumer class that runs a single consumer on any topic. The consumer evaluates
 * each incoming record based on the Conditions provided in the config. If and only If all
 * the conditions evaluate to true, only then all the configured [Action]s are fired.
 *
 * To be used with the model-invocation-endpoint and KSQL API_INFERENCE function to evaluate and
 * take action on incoming results from realtime inference on data.
 */
class InterventionMonitor(
    config: InterventionMonitorConfig,
    radar: RadarPropertyHandler,
    private val emailSenders: EmailSenders?,
) : AbstractKafkaMonitor<JsonNode, JsonNode, InterventionMonitorState>(
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
    }

    private fun configureConsumer() {
        val properties = Properties()
        val deserializer: String = KafkaJsonSchemaSerializer::class.java.name
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
                    emailExceptions()
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

    private fun emailExceptions() {
        emailSenders ?: return

        state.exceptions.forEach { (projectId, projectExceptions) ->
            val userMessages: List<String> = projectExceptions.exceptions
                .entries
                .map { (userId, userExceptions) ->
                    val exceptionList = userExceptions.lines
                    val numLines = exceptionList.size
                    if (userExceptions.isTruncated) {
                        exceptionList.addFirst("...")
                    }
                    val exceptionString = exceptionList.joinToString(separator = "") { "$exceptionPrefix$it" }
                    "user $userId - listing $numLines out of ${userExceptions.count} exceptions:$exceptionString"
                }

            val totalCount = projectExceptions.exceptions.values.sumOf { it.count }

            val date = LocalDate.ofInstant(state.fromDate, ZoneOffset.UTC).toString()

            val subject = "[RADAR-base $projectId] Errors in intervention algorithm on $date"
            val message = """
                Hi,
                
                On $date, some errors occurred in the RADAR-base intervention algorithm. This message summarizes the errors occurred for the RADAR-base $projectId project in the last 24 hours. A total of $totalCount errors were counted. Below is a summary of the errors:
                
                ${userMessages.joinToString(separator = "\n\n")}
                
                This is an automated message from the RADAR-base platform. Please refer to your RADAR-base administrator for more information.
                """.trimIndent()

            val sender = emailSenders.getEmailSenderForProject(projectId)

            if (sender != null) {
                sender.sendEmail(subject, message)
            } else {
                logger.error("No email sender configured for project {}. Not sending exception message.", projectId)
            }
        }
    }

    override fun evaluateRecord(record: ConsumerRecord<JsonNode, JsonNode>) {
        val intervention = parseRecord(record) ?: return

        if (intervention.exception.isNotEmpty()) {
            addException(intervention)
        }

        val interventionDeadline = intervention.time + deadline

        executor.execute {
            val userInterventions = state[intervention].interventions

            if (intervention.decision) {
                userInterventions += intervention.timeNotification
                if (userInterventions.size > maxInterventions) {
                    return@execute
                }
            } else {
                userInterventions -= intervention.timeNotification
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
                            sendNotification(
                                intervention = intervention,
                                interventionDeadline = interventionDeadline,
                            )
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
        executor.execute {
            state.addException(intervention)
        }
    }

    private fun parseRecord(record: ConsumerRecord<JsonNode, JsonNode>): InterventionRecord? {
        val userId = record.key().asText()
        if (userId.isEmpty()) {
            logger.error("Cannot map record without user ID")
            return null
        }
        val intervention = try {
            record.value().toInterventionRecord(userId)
        } catch (ex: IllegalArgumentException) {
            logger.error("Cannot map intervention record: {}", ex.toString())
            return null
        }
        if (intervention.time < state.fromDate) {
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
            get() = "$userId-$timeNotification"

        private const val exceptionPrefix = "\n - "
    }
}

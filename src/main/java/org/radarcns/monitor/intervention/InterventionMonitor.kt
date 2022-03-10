package org.radarcns.monitor.intervention

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import okhttp3.OkHttpClient
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarbase.appserver.client.protocol.FileProtocolDirectory
import org.radarcns.config.RadarPropertyHandler
import org.radarcns.config.monitor.InterventionMonitorConfig
import org.radarcns.monitor.AbstractKafkaMonitor
import org.radarcns.monitor.intervention.InterventionMonitorState.Companion.lastMidnight
import org.radarcns.util.EmailSenders
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
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
    private val config: InterventionMonitorConfig,
    radar: RadarPropertyHandler,
    emailSenders: EmailSenders?,
) : AbstractKafkaMonitor<String, GenericRecord, InterventionMonitorState>(
    radar,
    config.topics,
    "intervention_monitors",
    "1",
    InterventionMonitorState(),
) {
    private val queue: MutableMap<String, Future<*>> = HashMap()
    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val thresholdAdjust: ThresholdAdjustmentAlgorithm
    private val appServerNotifications: AppServerIntervention
    private var emailer: InterventionExceptionEmailer?
    private val exceptionBatch: LinkedBlockingQueue<InterventionRecord> = LinkedBlockingQueue()

    init {
        val httpClient = OkHttpClient()
        val mapper = jsonMapper {
            addModule(kotlinModule {
                enable(KotlinFeature.NullIsSameAsDefault)
                enable(KotlinFeature.NullToEmptyMap)
                enable(KotlinFeature.NullToEmptyCollection)
            })
            addModule(JavaTimeModule())
            disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        }
        appServerNotifications = AppServerIntervention(
            protocolDirectory = FileProtocolDirectory(config.protocolDirectory, mapper),
            defaultLanguage = config.defaultLanguage,
            cacheDuration = config.cacheDuration,
            appserverUrl = config.appServerUrl,
            authConfig = radar.radarProperties.auth,
            httpClient = httpClient,
            mapper = mapper,
        )

        thresholdAdjust = ThresholdAdjustmentAlgorithm(
            clientId = config.ksqlAppConfigClient,
            state = state,
            config = config.thresholdAdaptation,
            appConfigUrl = config.appConfigUrl,
            authConfig = radar.radarProperties.auth,
            httpClient = httpClient,
            mapper = mapper,
        )

        emailer = if (emailSenders != null) InterventionExceptionEmailer(
            emailSenders = emailSenders,
            state = state,
        ) else null
    }

    override fun start() {
        val resetInterval = config.stateResetInterval
        executor.execute {
            if (state.fromDate.passedDuration() > resetInterval) {
                emailer?.emailExceptions(state.fromDate + resetInterval)
                val lastMidnight = lastMidnight()
                val numberOfIterations = lastMidnight.passedDuration().dividedBy(resetInterval)
                state.reset(lastMidnight + resetInterval.multipliedBy(numberOfIterations))
                storeState()
            }

            // At the next interval, update the threshold levels
            val remainingInterval = (state.fromDate + resetInterval).pendingDuration()
            executor.scheduleAtFixedRate(
                {
                    logger.info("Updating intervention state")
                    val nextInterval = state.fromDate + resetInterval
                    try {
                        thresholdAdjust.updateThresholds()
                        emailer?.emailExceptions(nextInterval)
                        resetQueue()
                        state.reset(nextInterval)
                        storeState()
                    } catch (ex: Throwable) {
                        logger.error("Failed to update thresholds", ex)
                    }
                },
                remainingInterval.toMillis(),
                resetInterval.toMillis(),
                TimeUnit.MILLISECONDS,
            )
        }

        super.start()
    }

    override fun evaluateRecord(record: ConsumerRecord<String, GenericRecord>) {
        logger.info("Evaluating {}", record)
        val intervention = parseRecord(record) ?: return

        if (!intervention.exception.isNullOrEmpty()) {
            addException(intervention)
            return
        }

        val interventionDeadline = intervention.timeCompleted + config.deadline
        val interventionDeadlineTotal = interventionDeadline + config.ttlMargin

        executor.execute {
            try {
                queue.remove(intervention.queueKey)
                    ?.cancel(false)

                val interventionState = state[intervention]

                if (!intervention.decision) {
                    return@execute
                }

                logger.info("Scheduling intervention {}", intervention)

                val timeBeforeDeadlineTotal = interventionDeadlineTotal.pendingDuration()
                if (timeBeforeDeadlineTotal.isNegative) {
                    logger.info("For user {}, deadline for intervention {} has passed. Skipping.",
                        intervention.userId, interventionDeadline)
                    return@execute
                }
                if (intervention.isFinal) {
                    createAppMessages(intervention, interventionState, timeBeforeDeadlineTotal)
                } else {
                    queue[intervention.queueKey] = executor.schedule(
                        {
                            createAppMessages(
                                intervention,
                                interventionState,
                                interventionDeadlineTotal.pendingDuration()
                            )
                            queue -= intervention.queueKey
                        },
                        interventionDeadline.pendingDuration().toMillis(),
                        TimeUnit.MILLISECONDS,
                    )
                }
            } catch (ex: Throwable) {
                logger.error("Failed to process intervention $intervention", ex)
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
            try {
                val localExceptions = mutableListOf<InterventionRecord>()
                exceptionBatch.drainTo(localExceptions)
                localExceptions.forEach { state.addException(it) }
                super.afterEvaluate()
            } catch (ex: Throwable) {
                logger.error("Failed to run intervention afterEvaluate", ex)
            }
        }
    }

    private fun parseRecord(record: ConsumerRecord<String, GenericRecord>): InterventionRecord? {
        val userId = record.key()?.toString()
        if (userId.isNullOrEmpty()) {
            logger.error("Cannot map record without user ID")
            return null
        }
        val intervention = try {
            record.value().toInterventionRecord(userId)
        } catch (ex: Exception) {
            logger.error("Cannot map user {} intervention record value from {}: {}", userId, record.value(), ex.toString())
            return null
        }

        if (intervention.timeCompleted < state.fromDate) {
            return null
        }
        return intervention
    }

    private fun createAppMessages(
        intervention: InterventionRecord,
        interventionState: InterventionMonitorState.InterventionCount,
        ttl: Duration
    ) {
        if (!interventionState.interventions.add(intervention.time)) {
            logger.info("Already sent intervention for time point {}. Skipping.", intervention.time)
            return
        }
        if (interventionState.interventions.size > config.maxInterventions) {
            logger.info("For user {}, number of interventions {} would exceed maximum {}. Skipping.",
                intervention.userId, interventionState.interventions.size, config.maxInterventions)
            return
        } else {
            logger.info("Creating app notification for intervention {}", intervention.userId, intervention)
        }

        appServerNotifications.createMessages(intervention, ttl)
    }

    private fun resetQueue() {
        queue.values.forEach { it.cancel(false) }
        queue.clear()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(InterventionMonitor::class.java)

        fun Instant.pendingDuration(): Duration = Duration.between(Instant.now(), this)
        fun Instant.passedDuration(): Duration = Duration.between(this, Instant.now())

        private val InterventionRecord.queueKey: String
            get() = "$userId-$timeCompleted"

        private fun GenericRecord.toInterventionRecord(userId: String): InterventionRecord {
            val intervention = get("INTERVENTION") as GenericRecord
            return InterventionRecord(
                projectId = get("PROJECTID").toString(),
                userId = userId,
                sourceId = get("SOURCEID").toString(),
                time = (get("TIME") as Number).toLong(),
                timeCompleted = Instant.ofEpochMilli(((get("TIMECOMPLETED") as Number).toDouble() * 1000.0).toLong()),
                isFinal = get("ISFINAL") as Boolean,
                decision = intervention.get("DECISION") as Boolean,
                name = intervention.get("NAME")?.toString(),
                exception = intervention.get("EXCEPTION")?.toString(),
            )
        }
    }
}

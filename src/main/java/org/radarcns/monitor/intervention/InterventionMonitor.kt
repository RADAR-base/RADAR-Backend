package org.radarcns.monitor.intervention

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectReader
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import okhttp3.OkHttpClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.utils.Bytes
import org.radarbase.appserver.client.protocol.FileProtocolDirectory
import org.radarcns.config.RadarPropertyHandler
import org.radarcns.config.monitor.InterventionMonitorConfig
import org.radarcns.monitor.AbstractKafkaMonitor
import org.radarcns.monitor.intervention.InterventionMonitorState.Companion.lastMidnight
import org.radarcns.util.EmailSenders
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
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
    private val config: InterventionMonitorConfig,
    radar: RadarPropertyHandler,
    emailSenders: EmailSenders?,
) : AbstractKafkaMonitor<Bytes, Bytes, InterventionMonitorState>(
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
    private val keyReader: ObjectReader
    private val valueReader: ObjectReader

    init {
        configureConsumer()

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
        keyReader = mapper.readerFor(String::class.java)
        valueReader = mapper.readerFor(RawInterventionRecord::class.java)

        appServerNotifications = AppServerIntervention(
            protocolDirectory = FileProtocolDirectory(config.protocolDirectory, mapper),
            defaultLanguage = config.defaultLanguage,
            cacheDuration = config.cacheDuration,
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
        val deserializer: String = BytesDeserializer::class.java.name
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
        configure(properties)
    }

    override fun start() {
        executor.execute {
            if (state.fromDate.passedDuration() > config.stateResetInterval) {
                emailer?.emailExceptions()
                val lastMidnight = lastMidnight()
                val numberOfIterations = lastMidnight.passedDuration().dividedBy(config.stateResetInterval)
                state.reset(lastMidnight + config.stateResetInterval.multipliedBy(numberOfIterations))
                storeState()
            }

            // At the next interval, update the threshold levels
            val remainingInterval = (state.fromDate + config.stateResetInterval).pendingDuration()
            executor.scheduleAtFixedRate(
                {
                    logger.info("Updating intervention state")
                    try {
                        thresholdAdjust.updateThresholds()
                        emailer?.emailExceptions()
                        resetQueue()
                        state.reset(state.fromDate + config.stateResetInterval)
                        storeState()
                    } catch (ex: Throwable) {
                        logger.error("Failed to update thresholds", ex)
                    }
                },
                remainingInterval.toMillis(),
                config.stateResetInterval.toMillis(),
                TimeUnit.MILLISECONDS,
            )
        }

        super.start()
    }

    override fun evaluateRecord(record: ConsumerRecord<Bytes, Bytes>) {
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
                logger.info("Scheduling intervention {}", intervention)

                queue.remove(intervention.queueKey)
                    ?.cancel(false)

                val interventionState = state[intervention]

                if (!intervention.decision) {
                    return@execute
                }

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

    private fun parseRecord(record: ConsumerRecord<Bytes, Bytes>): InterventionRecord? {
        val userId = try {
            keyReader.readValue(record.key().get(), String::class.java)
        } catch (ex: Exception) {
            logger.error("Cannot map intervention record key from {}: {}", record.key(), ex.toString())
            return null
        }
        if (userId.isNullOrEmpty()) {
            logger.error("Cannot map record without user ID")
            return null
        }
        val intervention = try {
            valueReader.readValue(record.value().get(), RawInterventionRecord::class.java)
                ?.toInterventionRecord(userId)
        } catch (ex: Exception) {
            logger.error("Cannot map user {} intervention record value from {}: {}", userId, record.value(), ex.toString())
            return null
        }
        if (intervention == null) {
            logger.error("Cannot map user {} null intervention record value from {}", userId, record.value())
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
    }
}

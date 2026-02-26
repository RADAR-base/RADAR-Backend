package org.radarbase.stream

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.errors.StreamsException
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.radarbase.config.ConfigRadar
import org.radarbase.config.KafkaProperty
import org.radarbase.config.RadarPropertyHandler
import org.radarbase.config.SingleStreamConfig
import org.radarbase.topic.KafkaTopic
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.stream.Stream

abstract class AbstractStreamWorker : StreamWorker, Thread.UncaughtExceptionHandler {
    internal val streamDefinitions = CopyOnWriteArrayList<StreamDefinition>()
    protected lateinit var config: SingleStreamConfig
    protected lateinit var allConfig: ConfigRadar
    protected var numThreads: Int = 0
    protected lateinit var kafkaProperty: KafkaProperty
    protected lateinit var master: StreamMaster
    protected var streams: List<KafkaStreams>? = null

    protected fun defineStream(input: String, output: String) {
        defineStream(input, output, null)
    }

    protected fun defineStream(input: String, output: String, window: Duration?) {
        streamDefinitions.add(
            StreamDefinition(KafkaTopic(input), KafkaTopic(output), window)
        )
    }

    protected fun defineSensorStream(input: String) {
        defineStream(input, input + OUTPUT_LABEL, null)
    }

    protected fun defineWindowedSensorStream(input: String) {
        defineWindowedSensorStream(input, input)
    }

    protected fun defineWindowedSensorStream(input: String, outputBase: String) {
        // Since TimeWindowMetadata is in org.radarbase.stream package, we use it directly.
        // If it's not visible, we may need to use a fully qualified name or a helper.
        TimeWindowMetadata.values().forEach { w ->
            streamDefinitions.add(
                StreamDefinition(
                    KafkaTopic(input),
                    KafkaTopic(w.getTopicLabel(outputBase)),
                    Duration.ofMillis(w.intervalInMilliSec),
                    allConfig.stream!!.getCommitIntervalForTimeWindow(w)
                )
            )
        }
    }

    override fun getStreamDefinitions(): Stream<StreamDefinition> {
        return streamDefinitions.stream()
    }

    override fun configure(
        streamMaster: StreamMaster,
        properties: RadarPropertyHandler,
        singleConfig: SingleStreamConfig
    ) {
        this.kafkaProperty = properties.kafkaProperties
        this.allConfig = properties.radarProperties
        this.numThreads = allConfig.stream!!.threadsByPriority(singleConfig.priority)
        this.config = singleConfig
        this.master = streamMaster
        this.initialize()
    }

    override fun start() {
        if (streams != null) {
            throw IllegalStateException("Streams already started. Cannot start them again.")
        }
        streams = createStreams() ?: throw IllegalStateException("Streams are not initialized during start")

        streams?.forEach { stream ->
            stream.setUncaughtExceptionHandler(this as StreamsUncaughtExceptionHandler?)
            stream.start()
        }

        master.notifyStartedStream(this)
    }

    protected abstract fun createStreams(): List<KafkaStreams>?

    override fun shutdown() {
        logger.info("Shutting down {} stream", javaClass.simpleName)
        closeStreams()
        master.notifyClosedStream(this)
    }

    protected fun closeStreams() {
        streams?.forEach { it.close() }
        streams = null
        doCleanup()
    }

    protected abstract fun doCleanup()

    protected abstract fun initialize()

    override fun uncaughtException(t: Thread, e: Throwable) {
        logger.error("Thread {} has been terminated due to {}", t.name, e.message, e)
        closeStreams()
        if (e is StreamsException) {
            master.restartStream(this)
        } else {
            master.notifyCrashedStream(javaClass.simpleName)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AbstractStreamWorker::class.java)
        const val OUTPUT_LABEL = "_output"
        @JvmField
        val TIME_WINDOW_COMMIT_INTERVAL_DEFAULT: Duration = Duration.ofSeconds(30)
    }
}

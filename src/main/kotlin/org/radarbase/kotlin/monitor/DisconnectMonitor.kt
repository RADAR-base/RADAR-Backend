/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.kotlin.monitor

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.radarbase.kotlin.config.RadarPropertyHandler
import org.radarcns.kafka.ObservationKey
import org.radarbase.util.EmailSenders
import org.radarbase.kotlin.util.Monitor
import org.slf4j.LoggerFactory
import java.text.DateFormat
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.*
import javax.mail.MessagingException

/**
 * Monitors whether an ID has stopped sending measurements and sends an email when this occurs.
 */
class DisconnectMonitor(
    radar: RadarPropertyHandler,
    topics: Collection<String>,
    groupId: String,
    private val senders: EmailSenders?
) : AbstractKafkaMonitor<GenericRecord, GenericRecord, DisconnectMonitor.DisconnectMonitorState>(
    radar, topics, groupId, "1", DisconnectMonitorState()
) {
    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
    private val timeUntilReportedMissing: Duration
    private val dayFormat: DateFormat = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT, Locale.US)
    private val numRepetitions: Int
    private val repeatInterval: Duration
    private val minRepetitionInterval: Duration
    private val monitor: Monitor = Monitor(logger, " records monitored for Disconnect")
    private val message: String?

    init {
        val config = radar.radarProperties.disconnectMonitor
        timeUntilReportedMissing = Duration.ofSeconds(config!!.timeout)
        numRepetitions = config.alertRepetitions
        repeatInterval = Duration.ofSeconds(config.alertRepeatInterval)
        message = config.message
        minRepetitionInterval = repeatInterval.dividedBy(10)

        val props = Properties()
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        configure(props)

        pollTimeout = timeUntilReportedMissing
    }

    override fun start() {
        scheduler.scheduleAtFixedRate(monitor as Runnable, 5, 5, TimeUnit.MINUTES)
        startScheduler()
        super.start()
    }

    override fun shutdown() {
        super.shutdown()
        scheduler.shutdown()
    }

    internal fun startScheduler() {
        if (numRepetitions > 0) {
            logger.info("Start scheduled alert updates with the delay of {}", repeatInterval)
            state?.reportedMissing?.forEach { (key, report) -> scheduleRepetition(key, report) }
        }
    }

    override fun evaluateRecords(records: ConsumerRecords<GenericRecord, GenericRecord>) {
        super.evaluateRecords(records)

        val reportThreshold = Instant.now().minus(timeUntilReportedMissing)
        val iterator = state?.lastSeen?.entries?.iterator() ?: return

        while (iterator.hasNext()) {
            val entry = iterator.next()
            val lastSeen = entry.value
            if (reportThreshold.isAfter(Instant.ofEpochMilli(lastSeen))) {
                val missingKey = entry.key
                iterator.remove()
                reportMissing(missingKey, MissingRecordsReport(lastSeen))
            }
        }
    }

    override fun evaluateRecord(record: ConsumerRecord<GenericRecord, GenericRecord>) {
        val key = extractKey(record)
        monitor.increment()

        val now = System.currentTimeMillis()
        val keyString = getStateStore()!!.keyToString(key)
        state?.lastSeen?.put(keyString, now)

        val missingReport = state?.reportedMissing?.remove(keyString)
        if (missingReport != null) {
            missingReport.cancelRepetition()
            reportRecovered(key, missingReport.reportedMissing)
        }
    }

    private fun scheduleRepetition(key: String, report: MissingRecordsReport) {
        if (report.messageNumber < numRepetitions) {
            val reportedMissing = report.reportedMissing
            val now = Instant.now()
            val passedInterval = Duration.between(Instant.ofEpochMilli(reportedMissing), now)

            val nextRepetition = if (minRepetitionInterval >= repeatInterval.minus(passedInterval)) {
                minRepetitionInterval
            } else {
                repeatInterval.minus(passedInterval)
            }

            report.future = scheduler.schedule({ reportMissing(key, report.newRepetition()) },
                nextRepetition.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    private fun reportMissing(keyString: String, report: MissingRecordsReport) {
        val key = getStateStore()!!.stringToKey(keyString)
        val sender = senders?.getEmailSenderForProject(key.projectId) ?: return

        val timeout = report.timeout
        logger.info("Device {} timeout {} (message {} of {}). Reporting it missing.", key,
            timeout, report.messageNumber, numRepetitions)

        try {
            val lastSeen = dayFormat.format(report.lastSeenDate)
            var text = "The device $key seems disconnected. It was last seen on $lastSeen (${timeout / 1000L} seconds ago). If this is not intended, please ensure that it gets reconnected."
            if (message != null) {
                text += "\n\n$message"
            }
            var subject = "[RADAR] Device has disconnected"
            if (numRepetitions > 0 && report.messageNumber == numRepetitions) {
                text += "\n\nThis is the final warning email for this device."
                subject += ". Final message"
            } else if (numRepetitions > 0) {
                text += "\n\nThis is warning number ${report.messageNumber} of $numRepetitions"
            }

            sender.sendEmail(subject, text)
            logger.debug("Sent disconnected message successfully")
        } catch (mex: MessagingException) {
            logger.error("Failed to send disconnected message.", mex)
        } finally {
            state?.reportedMissing?.put(keyString, report)
            scheduleRepetition(keyString, report)
        }
    }

    private fun reportRecovered(key: ObservationKey, reportedMissingTime: Long) {
        val sender = senders?.getEmailSenderForProject(key.projectId) ?: return

        logger.info("Device {} seen again. Reporting it recovered.", key)
        try {
            val reportedMissingDate = Date(reportedMissingTime)
            val reportedMissing = dayFormat.format(reportedMissingDate)

            sender.sendEmail("[RADAR] device has reconnected",
                "The device $key that was reported disconnected on $reportedMissing has reconnected: it is sending new data.")
            logger.debug("Sent reconnected message successfully")
        } catch (mex: MessagingException) {
            logger.error("Failed to send reconnected message.", mex)
        }
    }

    class DisconnectMonitorState {
        val lastSeen: MutableMap<String, Long> = ConcurrentHashMap()
        val reportedMissing: MutableMap<String, MissingRecordsReport> = ConcurrentHashMap()

        fun setLastSeen(lastSeen: Map<String, Long>) {
            this.lastSeen.putAll(lastSeen)
        }

        fun setReportedMissing(reportedMissing: Map<String, MissingRecordsReport>) {
            this.reportedMissing.putAll(reportedMissing)
        }
    }

    class MissingRecordsReport @JsonCreator constructor(
        @JsonProperty("lastSeen") val lastSeen: Long,
        @JsonProperty("reportedMissing") val reportedMissing: Long,
        @JsonProperty("messageNumber") val messageNumber: Int
    ) {
        @JsonIgnore
        @Volatile
        var future: Future<*>? = null

        constructor(lastSeen: Long) : this(lastSeen, System.currentTimeMillis(), 0)

        @get:JsonIgnore
        val timeout: Long
            get() = reportedMissing - lastSeen

        @get:JsonIgnore
        val lastSeenDate: Date
            get() = Date(lastSeen)

        fun newRepetition(): MissingRecordsReport = MissingRecordsReport(lastSeen, System.currentTimeMillis(), messageNumber + 1)

        fun cancelRepetition() {
            future?.cancel(true)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DisconnectMonitor::class.java)
    }
}

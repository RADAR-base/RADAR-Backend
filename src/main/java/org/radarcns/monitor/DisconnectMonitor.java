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

package org.radarcns.monitor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.text.DateFormat;
import java.text.Format;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.mail.MessagingException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.config.monitor.DisconnectMonitorConfig;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.monitor.DisconnectMonitor.DisconnectMonitorState;
import org.radarcns.util.EmailSender;
import org.radarcns.util.EmailSenders;
import org.radarcns.util.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors whether an ID has stopped sending measurements and sends an email when this occurs.
 */
public class DisconnectMonitor extends AbstractKafkaMonitor<
        GenericRecord, GenericRecord, DisconnectMonitorState> {

    private static final Logger logger = LoggerFactory.getLogger(DisconnectMonitor.class);

    private final ScheduledExecutorService scheduler;
    private final Duration timeUntilReportedMissing;
    private final EmailSenders senders;
    private final Format dayFormat;
    private final int numRepetitions;
    private final Duration repeatInterval;
    private final Duration minRepetitionInterval;
    private final Monitor monitor;
    private final String message;

    public DisconnectMonitor(RadarPropertyHandler radar, Collection<String> topics, String groupId,
                             EmailSenders senders) {
        super(radar, topics, groupId, "1", new DisconnectMonitorState());
        this.senders = senders;
        this.dayFormat = DateFormat.getDateTimeInstance(
                DateFormat.MEDIUM, DateFormat.SHORT, Locale.US);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.monitor = new Monitor(logger, " records monitored for Disconnect");

        DisconnectMonitorConfig config = radar.getRadarProperties().getDisconnectMonitor();
        this.timeUntilReportedMissing = Duration.ofSeconds(config.getTimeout());
        this.numRepetitions = config.getAlertRepetitions();
        this.repeatInterval = Duration.ofSeconds(config.getAlertRepeatInterval());
        this.message = config.getMessage();
        this.minRepetitionInterval = repeatInterval.dividedBy(10);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        super.setPollTimeout(timeUntilReportedMissing);
    }

    /**
     * Schedules the repetitive alert task.
     */
    @Override
    public void start() {
        scheduler.scheduleAtFixedRate(this.monitor, 5, 5, TimeUnit.MINUTES);
        startScheduler();
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        scheduler.shutdown();
    }

    /**
     * Starts the scheduled alert updates.
     * Protected method to support unit testing
     */
    protected void startScheduler() {
        if (numRepetitions > 0) {
            logger.info("Start scheduled alert updates with the delay of {}", repeatInterval);
            state.reportedMissing.forEach(this::scheduleRepetition);
        }
    }

    @Override
    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        super.evaluateRecords(records);

        Instant reportThreshold = Instant.now().minus(timeUntilReportedMissing);

        Iterator<Map.Entry<String, Long>> iterator = state.lastSeen.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            // calculate timeout from current timestamp per device
            long lastSeen = entry.getValue();
            if (reportThreshold.isAfter(Instant.ofEpochMilli(lastSeen))) {
                String missingKey = entry.getKey();
                // remove processed records to prevent adding alerts multiple times
                iterator.remove();
                reportMissing(missingKey, new MissingRecordsReport(lastSeen));
            }
        }
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        ObservationKey key = extractKey(record);

        this.monitor.increment();

        long now = System.currentTimeMillis();
        String keyString = getStateStore().keyToString(key);
        state.lastSeen.put(keyString, now);

        MissingRecordsReport missingReport = state.reportedMissing.remove(keyString);
        if (missingReport != null) {
            missingReport.cancelRepetition();
            reportRecovered(key, missingReport.getReportedMissing());
        }
    }

    /**
     * Schedule a missing device message to be sent again.
     * @param key record key
     * @param report missing record details
     */
    private void scheduleRepetition(final String key, final MissingRecordsReport report) {
        if (report.getMessageNumber() < numRepetitions) {
            long reportedMissing = report.getReportedMissing();
            Instant now = Instant.now();
            Duration passedInterval = Duration.between(Instant.ofEpochMilli(reportedMissing), now);

            Duration nextRepetition;
            if (minRepetitionInterval.compareTo(repeatInterval.minus(passedInterval)) >= 0) {
                nextRepetition = minRepetitionInterval;
            } else {
                nextRepetition = repeatInterval.minus(passedInterval);
            }

            report.setFuture(scheduler.schedule(() -> reportMissing(key, report.newRepetition()),
                    nextRepetition.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    private void reportMissing(String keyString, MissingRecordsReport report) {
        ObservationKey key = getStateStore().stringToKey(keyString);

        // Don't report if no email address for this projectId
        EmailSender sender = senders.getEmailSenderForProject(key.getProjectId());
        if (sender == null) {
            return;
        }

        long timeout = report.getTimeout();
        logger.info("Device {} timeout {} (message {} of {}). Reporting it missing.", key,
                timeout, report.getMessageNumber(), numRepetitions);
        try {
            String lastSeen = dayFormat.format(report.getLastSeenDate());
            StringBuilder textBuilder = new StringBuilder(400);
            StringBuilder subjectBuilder = new StringBuilder(100);
            textBuilder.append("The device ")
                    .append(key)
                    .append(" seems disconnected. It was last seen on ")
                    .append(lastSeen)
                    .append(" (")
                    .append(String.valueOf(timeout / 1000L))
                    .append(" seconds ago). If this is not intended, please ensure that it gets reconnected.");
            if (message != null) {
                textBuilder.append("\n\n").append(message);
            }
            subjectBuilder.append("[RADAR] Device has disconnected");
            if (numRepetitions > 0 && report.getMessageNumber() == numRepetitions) {
                textBuilder.append("\n\nThis is the final warning email for this device.");
                subjectBuilder.append(". Final message");
            } else if (numRepetitions > 0) {
                textBuilder.append("\n\nThis is warning number ")
                        .append(String.valueOf(report.getMessageNumber()))
                        .append(" of ")
                        .append(String.valueOf(numRepetitions));
            }

            sender.sendEmail(subjectBuilder.toString(), textBuilder.toString());
            logger.debug("Sent disconnected message successfully");
        } catch (MessagingException mex) {
            logger.error("Failed to send disconnected message.", mex);
        } finally {
            // store last seen and reportedMissing timestamp
            state.getReportedMissing().put(keyString, report);
            scheduleRepetition(keyString, report);
        }
    }

    private void reportRecovered(ObservationKey key, long reportedMissingTime) {
        // Don't report if no email address for this projectId
        EmailSender sender = senders.getEmailSenderForProject(key.getProjectId());
        if (sender == null) {
            return;
        }

        logger.info("Device {} seen again. Reporting it recovered.", key);
        try {
            Date reportedMissingDate = new Date(reportedMissingTime);
            String reportedMissing = dayFormat.format(reportedMissingDate);

            sender.sendEmail("[RADAR] device has reconnected",
                    "The device " + key + " that was reported disconnected on "
                            + reportedMissing + " has reconnected: it is sending new data.");
            logger.debug("Sent reconnected message successfully");
        } catch (MessagingException mex) {
            logger.error("Failed to send reconnected message.", mex);
        }
    }

    /**
     * State of disconnect monitor.
     */
    public static class DisconnectMonitorState {
        private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
        private final Map<String, MissingRecordsReport> reportedMissing = new ConcurrentHashMap<>();

        public Map<String, Long> getLastSeen() {
            return lastSeen;
        }

        public void setLastSeen(Map<String, Long> lastSeen) {
            this.lastSeen.putAll(lastSeen);
        }

        public Map<String, MissingRecordsReport> getReportedMissing() {
            return reportedMissing;
        }

        public void setReportedMissing(Map<String, MissingRecordsReport> reportedMissing) {
            this.reportedMissing.putAll(reportedMissing);
        }
    }

    /**
     * Stores data of data from missing records alert
     * such as lastSeen and reportedTime.
     */
    public static class MissingRecordsReport {
        private final long lastSeen;
        private final long reportedMissing;
        private final int messageNumber;

        @JsonIgnore
        private Future<?> future;

        @JsonCreator
        public MissingRecordsReport(@JsonProperty("lastSeen") long lastSeen,
                @JsonProperty("reportedMissing") long reportedMissing,
                @JsonProperty("messagesProcessed") int messageNumber) {
            this.lastSeen = lastSeen;
            this.reportedMissing = reportedMissing;
            this.messageNumber = messageNumber;
            this.future = null;
        }

        public MissingRecordsReport(long lastSeen) {
            this(lastSeen, System.currentTimeMillis(), 0);
        }

        public long getLastSeen() {
            return lastSeen;
        }

        public long getReportedMissing() {
            return reportedMissing;
        }

        @JsonIgnore
        public long getTimeout() {
            return reportedMissing - lastSeen;
        }

        @JsonIgnore
        public Date getLastSeenDate() {
            return new Date(lastSeen);
        }

        public int getMessageNumber() {
            return messageNumber;
        }

        public MissingRecordsReport newRepetition() {
            return new MissingRecordsReport(
                    lastSeen, System.currentTimeMillis(), messageNumber + 1);
        }

        public synchronized void cancelRepetition() {
            if (future != null) {
                future.cancel(true);
            }
        }

        public synchronized void setFuture(Future<?> future) {
            this.future = future;
        }
    }
}

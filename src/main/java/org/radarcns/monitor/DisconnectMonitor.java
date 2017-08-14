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
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.config.DisconnectMonitorConfig;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.key.MeasurementKey;
import org.radarcns.monitor.DisconnectMonitor.DisconnectMonitorState;
import org.radarcns.util.EmailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.MessagingException;
import java.text.DateFormat;
import java.text.Format;
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

import static org.radarcns.util.PersistentStateStore.measurementKeyToString;
import static org.radarcns.util.PersistentStateStore.stringToKey;

/**
 * Monitors whether an ID has stopped sending measurements and sends an email when this occurs.
 */
public class DisconnectMonitor extends AbstractKafkaMonitor<
        GenericRecord, GenericRecord, DisconnectMonitorState> {

    private static final Logger logger = LoggerFactory.getLogger(DisconnectMonitor.class);

    private final ScheduledExecutorService scheduler;
    private final long timeUntilReportedMissing;
    private final EmailSender sender;
    private final Format dayFormat;
    private final int logInterval;
    private final int numRepetitions;
    private final long repetitionInterval;
    private final long minRepetitionInterval;
    private long messagesProcessed;

    public DisconnectMonitor(RadarPropertyHandler radar, Collection<String> topics, String groupId,
            EmailSender sender) {
        super(radar, topics, groupId, "1", new DisconnectMonitorState());
        this.sender = sender;
        this.dayFormat = DateFormat.getDateTimeInstance(
                DateFormat.MEDIUM, DateFormat.SHORT, Locale.US);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        DisconnectMonitorConfig config = radar.getRadarProperties().getDisconnectMonitor();
        this.timeUntilReportedMissing = config.getTimeout() * 1000L;
        this.logInterval = config.getLogInterval();
        this.numRepetitions = config.getAlertRepetitions();
        this.repetitionInterval = config.getAlertRepeatInterval() * 1000L;
        this.minRepetitionInterval = repetitionInterval / 10;

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        super.setPollTimeout(timeUntilReportedMissing);
    }

    /**
     * Schedules the repeatitive alert task
     */
    @Override
    public void start() {
        startScheduler();
        super.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        scheduler.shutdown();
    }

    /**
     * starts the scheduled alert updates.
     * Protected method to support unit testing
     */
    protected void startScheduler() {
        if (scheduler != null && numRepetitions > 0) {
            logger.info("Start scheduled alert updates with the delay of {}", repetitionInterval);
            for (Map.Entry<String, MissingRecordsReport> entry : state.reportedMissing.entrySet()) {
                scheduleRepeat(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        super.evaluateRecords(records);

        long now = System.currentTimeMillis();

        Iterator<Map.Entry<String, Long>> iterator = state.lastSeen.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            // calculate timeout from current timestamp per device
            long lastSeen = entry.getValue();
            if (now - lastSeen > timeUntilReportedMissing) {
                String missingKey = entry.getKey();
                // remove processed records to prevent adding alerts multiple times
                iterator.remove();
                reportMissing(missingKey, new MissingRecordsReport(lastSeen));
            }
        }
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        MeasurementKey key = extractKey(record);

        if (logInterval > 0 && ((int) (messagesProcessed % logInterval)) == 0) {
            logger.info("Evaluating connection status of record offset {} of {} with value {}",
                    record.offset(), key, record.value());
        }
        messagesProcessed++;

        long now = System.currentTimeMillis();
        String keyString = measurementKeyToString(key);
        state.lastSeen.put(keyString, now);

        MissingRecordsReport missingRecord = state.reportedMissing.remove(keyString);
        if (missingRecord != null) {
            missingRecord.cancelRepetition();
            reportRecovered(key, missingRecord.getReportedMissing());
        }
    }

    private void scheduleRepeat(final String missingKey, final MissingRecordsReport missingRecord) {
        if (scheduler != null && missingRecord.messageNumber < numRepetitions) {
            long passedInterval = System.currentTimeMillis() - missingRecord.reportedMissing;
            long nextRepetition = Math.max(minRepetitionInterval,
                    repetitionInterval - passedInterval);

            missingRecord.setFuture(scheduler.schedule(
                    () -> reportMissing(missingKey, missingRecord.newRepetition()),
                    nextRepetition, TimeUnit.MILLISECONDS));
        }
    }

    private void reportMissing(String keyString, MissingRecordsReport record) {
        MeasurementKey key = stringToKey(keyString);
        long timeout = record.getTimeout();
        logger.info("Device {} timeout {} (message {} of {}). Reporting it missing.", key,
                timeout, record.messageNumber, numRepetitions);
        try {
            String lastSeen = dayFormat.format(record.getLastSeenDate());
            String text = "The device " + key + " seems disconnected. "
                    + "It was last seen on " + lastSeen + " (" + timeout / 1000L
                    + " seconds ago). If this is not intended, please ensure that it gets "
                    + "reconnected.";
            String subject = "[RADAR] Device has disconnected";
            if (numRepetitions > 0 && record.messageNumber == numRepetitions) {
                text += "\n\nThis is the final warning email for this device.";
                subject += ". Final message";
            } else if (numRepetitions > 0) {
                text += "\n\nThis is warning number " + record.messageNumber + " of "
                        + numRepetitions;
            }

            sender.sendEmail(subject, text);
            logger.debug("Sent disconnected message successfully");
        } catch (MessagingException mex) {
            logger.error("Failed to send disconnected message.", mex);
        } finally {
            // store last seen and reportedMissing timestamp
            state.reportedMissing.put(keyString, record);
            scheduleRepeat(keyString, record);
        }
    }

    private void reportRecovered(MeasurementKey key, long reportedMissingTime) {
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
     * State of disconnect monitor
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
     * such as lastSeen and reportedTime
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

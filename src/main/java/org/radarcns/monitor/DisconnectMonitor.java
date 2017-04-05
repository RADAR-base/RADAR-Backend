/*
 * Copyright 2017 Kings College London and The Hyve
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

import static org.radarcns.util.PersistentStateStore.measurementKeyToString;
import static org.radarcns.util.PersistentStateStore.stringToKey;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import javax.mail.MessagingException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.key.MeasurementKey;
import org.radarcns.monitor.DisconnectMonitor.DisconnectMonitorState;
import org.radarcns.util.EmailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors whether an ID has stopped sending measurements and sends an email when this occurs.
 */
public class DisconnectMonitor extends AbstractKafkaMonitor<
        GenericRecord, GenericRecord, DisconnectMonitorState> {
    private static final Logger logger = LoggerFactory.getLogger(DisconnectMonitor.class);

    private final long timeUntilReportedMissing;
    private final EmailSender sender;
    private final Format dayFormat;

    public DisconnectMonitor(RadarPropertyHandler radar, Collection<String> topics, String groupId,
            EmailSender sender, long timeUntilReportedMissing) {
        super(radar, topics, groupId, "1", new DisconnectMonitorState());
        this.timeUntilReportedMissing = timeUntilReportedMissing;
        this.sender = sender;

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        super.setPollTimeout(timeUntilReportedMissing);
        dayFormat = new SimpleDateFormat("EEE, d MMM 'at' HH:mm:ss z", Locale.US);
    }

    @Override
    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        super.evaluateRecords(records);

        long now = System.currentTimeMillis();

        Iterator<Map.Entry<String, Long>> iterator = state.lastSeen.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            long timeout = now - entry.getValue();
            if (timeout > timeUntilReportedMissing) {
                String missingKey = entry.getKey();
                iterator.remove();
                reportMissing(stringToKey(missingKey), timeout);
                state.reportedMissing.put(missingKey, now);
            }
        }
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        MeasurementKey key = extractKey(record);

        long now = System.currentTimeMillis();
        String keyString = measurementKeyToString(key);
        state.lastSeen.put(keyString, now);

        Long reportedMissingTime = state.reportedMissing.remove(keyString);
        if (reportedMissingTime != null) {
            reportRecovered(key, reportedMissingTime);
        }
    }

    private void reportMissing(MeasurementKey key, long timeout) {
        logger.info("Device {} timeout {}. Reporting it missing.", key, timeout);
        try {
            Date lastSeenDate = new Date(System.currentTimeMillis() - timeout);
            String lastSeen = dayFormat.format(lastSeenDate);
            sender.sendEmail("[RADAR-CNS] device has disconnected",
                    "The device " + key + " seems disconnected. "
                            + "It was last seen on " + lastSeen + " (" + timeout / 1000L
                            + " seconds ago). If this is not intended, please ensure that it gets "
                            + "reconnected.");
            logger.debug("Sent disconnected message successfully");
        } catch (MessagingException mex) {
            logger.error("Failed to send disconnected message.", mex);
        }
    }

    private void reportRecovered(MeasurementKey key, long reportedMissingTime) {
        logger.info("Device {} seen again. Reporting it recovered.", key);
        try {
            Date reportedMissingDate = new Date(reportedMissingTime);
            String reportedMissing = dayFormat.format(reportedMissingDate);

            sender.sendEmail("[RADAR-CNS] device has reconnected",
                    "The device " + key + " that was reported disconnected on "
                            + reportedMissing + " has reconnected: it is sending new data.");
            logger.debug("Sent reconnected message successfully");
        } catch (MessagingException mex) {
            logger.error("Failed to send reconnected message.", mex);
        }
    }

    public static class DisconnectMonitorState {
        private final Map<String, Long> lastSeen = new HashMap<>();
        private final Map<String, Long> reportedMissing = new HashMap<>();

        public Map<String, Long> getLastSeen() {
            return lastSeen;
        }

        public void setLastSeen(Map<String, Long> lastSeen) {
            this.lastSeen.putAll(lastSeen);
        }

        public Map<String, Long> getReportedMissing() {
            return reportedMissing;
        }

        public void setReportedMissing(Map<String, Long> reportedMissing) {
            this.reportedMissing.putAll(reportedMissing);
        }
    }
}

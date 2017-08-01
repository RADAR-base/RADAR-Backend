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

import static org.radarcns.util.PersistentStateStore.measurementKeyToString;
import static org.radarcns.util.PersistentStateStore.missingRecordsReportToString;
import static org.radarcns.util.PersistentStateStore.stringToKey;
import static org.radarcns.util.PersistentStateStore.stringToMissingRecordsReport;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    private ScheduledExecutorService scheduler ;
    private final long timeUntilReportedMissing;
    private final EmailSender sender;
    private final Format dayFormat;
    private final long logInterval;
    private long messageNumber;
    private long periodicalDelay = 3600_000L; // 1 hour

    public DisconnectMonitor(RadarPropertyHandler radar, Collection<String> topics, String groupId,
            EmailSender sender, long timeUntilReportedMissing, long logInterval , ScheduledExecutorService scheduler) {
        super(radar, topics, groupId, "1", new DisconnectMonitorState());
        this.timeUntilReportedMissing = timeUntilReportedMissing;
        this.sender = sender;
        this.logInterval = logInterval;
        this.dayFormat = new SimpleDateFormat("EEE, d MMM 'at' HH:mm:ss z", Locale.US);
        this.scheduler = scheduler;
        if(radar.getRadarProperties().getDisconnectMonitor()!=null && radar.getRadarProperties().getDisconnectMonitor().getRepetitiveAlertDelay() !=null) {
            this.periodicalDelay = radar.getRadarProperties().getDisconnectMonitor().getRepetitiveAlertDelay();
        }

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

    /**
     * starts the scheduled alert updates.
     * Protected method to support unit testing
     */
    protected void startScheduler() {
        if(scheduler !=null ) {
            logger.info("Start scheduler");
            scheduler.scheduleWithFixedDelay(new ScheduledAlerts(), periodicalDelay, periodicalDelay,
                TimeUnit.MILLISECONDS);
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
            long timeout = now - lastSeen;
            if (timeout > timeUntilReportedMissing) {
                String missingKey = entry.getKey();
                // remove processed records to prevent repeatitive alerts
                iterator.remove();
                reportMissing(stringToKey(missingKey), timeout);
                try {
                  // store last seen and reportedMissing timestamp
                    state.reportedMissing.put(missingKey, missingRecordsReportToString(new MissingRecordsReport(lastSeen , now)));
                } catch (IOException e) {
                    logger.error("Cannot serialize missing records data");
                }

            }

        }
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        MeasurementKey key = extractKey(record);

        if (logInterval > 0 && ((int) (messageNumber % logInterval)) == 0) {
            logger.info("Evaluating connection status of record offset {} of {} with value {}",
                    record.offset(), key, record.value());
        }
        messageNumber++;

        long now = System.currentTimeMillis();
        String keyString = measurementKeyToString(key);
        state.lastSeen.put(keyString, now);

        String reportedMissingTime = state.reportedMissing.remove(keyString);
        if (reportedMissingTime != null) {
            try {
                reportRecovered(key, stringToMissingRecordsReport(reportedMissingTime).getReportedMissing());
            } catch (IOException e) {
               logger.error("Cannot deserialize missing report data");
            }
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

  /**
   * State of disconnect monitor
   */
  public static class DisconnectMonitorState {
        private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
        private final Map<String, String> reportedMissing = new ConcurrentHashMap<>();

        public Map<String, Long> getLastSeen() {
            return lastSeen;
        }

        public void setLastSeen(Map<String, Long> lastSeen) {
            this.lastSeen.putAll(lastSeen);
        }

        public Map<String, String> getReportedMissing() {
            return reportedMissing;
        }

        public void setReportedMissing(Map<String, String> reportedMissing) {
            this.reportedMissing.putAll(reportedMissing);
        }
    }

  /**
   * Stores data of data from missing records alert
   * such as lastSeen and reportedTime
   */
  public static class MissingRecordsReport {
      private Long lastSeen;
      private Long reportedMissing;

      public MissingRecordsReport() {}
      MissingRecordsReport(Long lastSeen , Long reportedMissing) {
        this.lastSeen = lastSeen;
        this.reportedMissing = reportedMissing;
      }

      public Long getLastSeen() {
        return lastSeen;
      }

      public Long getReportedMissing() {
        return reportedMissing;
      }

      public void setLastSeen(Long lastSeen) {
            this.lastSeen = lastSeen;
        }

        public void setReportedMissing(Long reportedMissing) {
            this.reportedMissing = reportedMissing;
        }
    }

  /**
   * Task that runs with scheduled delays to report if the device is still disconnected
   */
  private class ScheduledAlerts implements Runnable{
        @Override
        public void run(){

            long now = System.currentTimeMillis();
            logger.info("Scheduled alert updates at " + now);
            Iterator<Map.Entry<String, String>> iterator = state.reportedMissing.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                MissingRecordsReport missingRecordsReport = null;
                try {
                    missingRecordsReport = stringToMissingRecordsReport(entry.getValue());
                    long timeout = now - missingRecordsReport.getLastSeen();
                    if (timeout > timeUntilReportedMissing) {
                        String missingKey = entry.getKey();
                        reportMissing(stringToKey(missingKey), timeout);
                        state.reportedMissing.put(missingKey, missingRecordsReportToString(new MissingRecordsReport(missingRecordsReport.getLastSeen() , now)));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}

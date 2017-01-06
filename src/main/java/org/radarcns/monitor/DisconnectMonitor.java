package org.radarcns.monitor;

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
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        MeasurementKey key = extractKey(record);

        long now = System.currentTimeMillis();
        state.lastSeen.put(key, now);

        Long reportedMissingTime = state.reportedMissing.remove(key);
        if (reportedMissingTime != null) {
            reportRecovered(key, reportedMissingTime);
        }

        Iterator<Map.Entry<MeasurementKey, Long>> iterator = state.lastSeen.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<MeasurementKey, Long> entry = iterator.next();
            long timeout = entry.getValue() - now;
            if (timeout > timeUntilReportedMissing) {
                MeasurementKey missingKey = entry.getKey();
                iterator.remove();
                logger.info("Device {} timeout {}. Reporting it missing.", missingKey, timeout);
                reportMissing(missingKey, timeout);
                state.reportedMissing.put(missingKey, now);
            }
        }
    }

    private void reportMissing(MeasurementKey key, long timeout) {
        try {
            Date lastSeenDate = new Date(System.currentTimeMillis() - timeout);
            String lastSeen = dayFormat.format(lastSeenDate);
            sender.sendEmail("[RADAR-CNS] device has disconnected",
                    "The device " + key + " seems disconnected. "
                            + "It was last seen on " + lastSeen + " (" + timeout / 1000L
                            + " seconds ago). If this is not intended, please ensure that it gets "
                            + "reconnected.");
            logger.info("Sent disconnected message successfully");
        } catch (MessagingException mex) {
            logger.error("Failed to send disconnected message.", mex);
        }
    }

    private void reportRecovered(MeasurementKey key, long reportedMissingTime) {
        try {
            Date reportedMissingDate = new Date(reportedMissingTime);
            String reportedMissing = dayFormat.format(reportedMissingDate);

            sender.sendEmail("[RADAR-CNS] device has reconnected",
                    "The device " + key + " that was reported disconnected on "
                            + reportedMissing + " has reconnected: it is sending new data.");
            logger.info("Sent reconnected message successfully");
        } catch (MessagingException mex) {
            logger.error("Failed to send reconnected message.", mex);
        }
    }

    public static class DisconnectMonitorState {
        private final Map<MeasurementKey, Long> lastSeen = new HashMap<>();
        private final Map<MeasurementKey, Long> reportedMissing = new HashMap<>();

        public Map<MeasurementKey, Long> getLastSeen() {
            return lastSeen;
        }

        public void setLastSeen(Map<MeasurementKey, Long> lastSeen) {
            this.lastSeen.putAll(lastSeen);
        }

        public Map<MeasurementKey, Long> getReportedMissing() {
            return reportedMissing;
        }

        public void setReportedMissing(
                Map<MeasurementKey, Long> reportedMissing) {
            this.reportedMissing.putAll(reportedMissing);
        }
    }
}

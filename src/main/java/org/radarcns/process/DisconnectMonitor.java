package org.radarcns.process;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.mail.MessagingException;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.key.MeasurementKey;
import org.radarcns.util.EmailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors whether an ID has stopped sending measurements and sends an email when this occurs.
 */
public class DisconnectMonitor extends AbstractKafkaMonitor<GenericRecord, GenericRecord> {
    private final long timeUntilReportedMissing;
    private final Map<MeasurementKey, Long> lastSeen;
    private final Map<MeasurementKey, Long> reportedMissing;
    private static final Logger logger = LoggerFactory.getLogger(DisconnectMonitor.class);
    private final EmailSender sender;
    private final Format dayFormat;

    public DisconnectMonitor(List<String> topics, String clientID, long timeUntilReportedMissing,
            EmailSender sender) {
        super(topics, clientID);
        this.timeUntilReportedMissing = timeUntilReportedMissing;
        lastSeen = new HashMap<>();
        this.sender = sender;

        super.setPollTimeout(timeUntilReportedMissing);
        reportedMissing = new HashMap<>();
        dayFormat = new SimpleDateFormat("EEE, d MMM at HH:mm:ss z");
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        MeasurementKey key = extractKey(record);

        long now = System.currentTimeMillis();
        lastSeen.put(key, now);

        Long reportedMissingTime = reportedMissing.remove(key);
        if (reportedMissingTime != null) {
            reportRecovered(key, reportedMissingTime);
        }

        Iterator<Map.Entry<MeasurementKey, Long>> iterator = lastSeen.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<MeasurementKey, Long> entry = iterator.next();
            long timeout = entry.getValue() - now;
            if (timeout > timeUntilReportedMissing) {
                MeasurementKey missingKey = entry.getKey();
                iterator.remove();
                logger.info("Device {} timeout {}. Reporting it missing.", missingKey, timeout);
                reportMissing(missingKey, timeout);
                reportedMissing.put(missingKey, now);
            }
        }
    }

    private long extractTimeReceived(GenericRecord value, long defaultValue) {
        Field timeReceivedField = value.getSchema().getField("timeReceived");
        long reportedTime;
        if (timeReceivedField != null) {
            double timeReceivedValue = ((Number) value.get(timeReceivedField.pos())).doubleValue();
            reportedTime = Math.round(1000d * timeReceivedValue);
        } else {
            reportedTime = defaultValue;
        }
        return reportedTime;
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
}

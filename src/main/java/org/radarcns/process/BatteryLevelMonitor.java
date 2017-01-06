package org.radarcns.process;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.mail.MessagingException;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.key.MeasurementKey;
import org.radarcns.util.EmailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Monitors the battery level for any devices running empty */
public class BatteryLevelMonitor extends AbstractKafkaMonitor<GenericRecord, GenericRecord> {
    private final Set<MeasurementKey> isLow;
    private final Set<MeasurementKey> isCritical;
    private static final Logger logger = LoggerFactory.getLogger(BatteryLevelMonitor.class);
    private final EmailSender sender;
    private final Status minLevel;

    public BatteryLevelMonitor(String topic, EmailSender sender, Status minLevel) {
        super(Collections.singletonList(topic), "1");

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "battery_monitors");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        this.isLow = new HashSet<>();
        this.isCritical = new HashSet<>();
        this.sender = sender;
        this.minLevel = minLevel == null ? Status.CRITICAL : minLevel;
    }

    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        try {
            MeasurementKey key = extractKey(record);
            float batteryLevel = extractBatteryLevel(record);
            if (batteryLevel <= Status.CRITICAL.getLevel()) {
                if (isCritical.add(key)) {
                    isLow.add(key);
                    updateStatus(key, Status.CRITICAL);
                    logger.warn("Battery level of sensor {} of user {} is critically low",
                            key.getSourceId(), key.getUserId());
                }
            } else if (batteryLevel < Status.LOW.getLevel()) {
                if (isLow.add(key)) {
                    updateStatus(key, Status.LOW);
                    logger.warn("Battery level of sensor {} of user {} is low",
                            key.getSourceId(), key.getUserId());
                }
            } else if (isLow.remove(key)) {
                isCritical.remove(key);
                updateStatus(key, Status.NORMAL);
                logger.info("Battery of sensor {} of user {} is has returned to normal.",
                        key.getSourceId(), key.getUserId());
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Failed to process record {}", record, ex);
        }
    }

    private void updateStatus(MeasurementKey key, Status status) {
        if (sender != null && status.getLevel() <= minLevel.getLevel()) {
            try {
                sender.sendEmail("[RADAR-CNS] battery level low",
                        "The battery level of " + key + " is now " + status + ". "
                                + "Please ensure that it gets recharged.");
                logger.info("Sent battery level status message successfully");
            } catch (MessagingException mex) {
                logger.error("Failed to send battery level status message.", mex);
            }
        }
    }

    private float extractBatteryLevel(ConsumerRecord<?, GenericRecord> record) {
        GenericRecord value = record.value();
        Field batteryField = value.getSchema().getField("batteryLevel");
        if (batteryField == null) {
            throw new IllegalArgumentException("Failed to process record with value type " +
                    value.getSchema() + " without batteryLevel field.");
        }
        Number batteryLevel = (Number) record.value().get(batteryField.pos());
        return batteryLevel.floatValue();
    }

    public static void main(String[] args) {
        BatteryLevelMonitor monitor = new BatteryLevelMonitor(
                "android_empatica_e4_battery_level", null, null);
        monitor.start();
    }

    public enum Status {
        NORMAL(1.0f), LOW(0.2f), CRITICAL(0.05f), EMPTY(0f);

        private final float level;

        Status(float level) {
            this.level = level;
        }

        public float getLevel() {
            return this.level;
        }
    }
}

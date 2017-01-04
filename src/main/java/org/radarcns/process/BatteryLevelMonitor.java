package org.radarcns.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.key.MeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Monitors the battery level for any devices running empty */
public class BatteryLevelMonitor extends AbstractKafkaMonitor<GenericRecord, GenericRecord> {
    private final Set<MeasurementKey> isLow;
    private final Set<MeasurementKey> isCritical;
    private final List<BatteryLevelListener> listeners;
    private static final Logger logger = LoggerFactory.getLogger(BatteryLevelMonitor.class);

    public BatteryLevelMonitor(String topic) {
        super(Collections.singletonList(topic), "1");

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "battery_monitors");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        this.isLow = new HashSet<>();
        this.isCritical = new HashSet<>();
        this.listeners = new ArrayList<>();
    }

    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
            try {
                MeasurementKey key = extractKey(record);
                float batteryLevel = extractBatteryLevel(record);
                if (batteryLevel < 0.05) {
                    boolean newlyCritical = isCritical.add(key);
                    if (newlyCritical) {
                        isLow.add(key);
                        for (BatteryLevelListener listener : listeners) {
                            listener.batteryLevelStatusUpdated(key,
                                    BatteryLevelListener.Status.CRITICAL);
                        }
                    }
                } else if (batteryLevel < 0.2) {
                    if (isLow.add(key)) {
                        for (BatteryLevelListener listener : listeners) {
                            listener.batteryLevelStatusUpdated(key,
                                    BatteryLevelListener.Status.LOW);
                        }
                    }
                } else {
                    if (isLow.remove(key)) {
                        isCritical.remove(key);
                        for (BatteryLevelListener listener : listeners) {
                            listener.batteryLevelStatusUpdated(key,
                                    BatteryLevelListener.Status.NORMAL);
                        }
                    }
                }
            } catch (IllegalArgumentException ex) {
                logger.error("Failed to process record {}", record, ex);
            }
        }
    }

    private MeasurementKey extractKey(ConsumerRecord<GenericRecord, GenericRecord> record) {
        GenericRecord key = record.key();
        if (key == null) {
            throw new IllegalArgumentException("Failed to process record without a key.");
        }
        Schema keySchema = key.getSchema();
        Field userIdField = keySchema.getField("userId");
        if (userIdField == null) {
            throw new IllegalArgumentException("Failed to process record with key type "
                    + key.getSchema() + " without user ID.");
        }
        Field sourceIdField = keySchema.getField("sourceId");
        if (sourceIdField == null) {
            throw new IllegalArgumentException("Failed to process record with key type "
                    + key.getSchema() + " without source ID.");
        }
        return new MeasurementKey(
                (String) key.get(userIdField.pos()),
                (String) key.get(sourceIdField.pos()));
    }

    private float extractBatteryLevel(ConsumerRecord<GenericRecord, GenericRecord> record) {
        GenericRecord value = record.value();
        Field batteryField = value.getSchema().getField("batteryLevel");
        if (batteryField == null) {
            throw new IllegalArgumentException("Failed to process record with value type " +
                    value.getSchema() + " without batteryLevel field.");
        }
        Number batteryLevel = (Number) record.value().get(batteryField.pos());
        return batteryLevel.floatValue();
    }

    public void addBatteryLevelListener(BatteryLevelListener listener) {
        this.listeners.add(listener);
    }

    public static void main(String[] args) {
        BatteryLevelMonitor monitor = new BatteryLevelMonitor("android_empatica_e4_battery_level");
        monitor.addBatteryLevelListener(new BatteryLevelLogger());
        monitor.start();
    }
}

package org.radarcns.process;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.key.MeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** Monitors the battery level for any devices running empty */
public class BatteryLevelMonitor extends KafkaMonitor<GenericRecord, GenericRecord> {
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
            GenericRecord key = record.key();
            if (key == null) {
                logger.error("Failed to process record {} without a key.", record);
                return;
            }
            MeasurementKey measurementKey;
            Schema keySchema = key.getSchema();
            if (keySchema.getField("userId") != null
                    && keySchema.getField("sourceId") != null) {
                measurementKey = new MeasurementKey((String) key.get("userId"),
                        (String) key.get("sourceId"));
            } else {
                logger.error("Failed to process record {} with wrong key type {}.",
                        record, key.getSchema());
                return;
            }
            GenericRecord value = record.value();
            Schema recordSchema = value.getSchema();

            int batteryLevelFieldId = recordSchema.getField("batteryLevel").pos();
            Number batteryLevel = (Number) value.get(batteryLevelFieldId);
            if (batteryLevel.floatValue() < 0.05) {
                boolean newlyCritical = isCritical.add(measurementKey);
                if (newlyCritical) {
                    isLow.add(measurementKey);
                    for (BatteryLevelListener listener : listeners) {
                        listener.batteryLevelStatusUpdated(measurementKey,
                                BatteryLevelListener.Status.CRITICAL);
                    }
                }
            } else if (batteryLevel.floatValue() < 0.2) {
                if (isLow.add(measurementKey)) {
                    for (BatteryLevelListener listener : listeners) {
                        listener.batteryLevelStatusUpdated(measurementKey,
                                BatteryLevelListener.Status.LOW);
                    }
                }
            } else {
                if (isLow.remove(measurementKey)) {
                    isCritical.remove(measurementKey);
                    for (BatteryLevelListener listener : listeners) {
                        listener.batteryLevelStatusUpdated(measurementKey,
                                BatteryLevelListener.Status.NORMAL);
                    }
                }
            }
        }
    }

    public void addBatteryLevelListener(BatteryLevelListener listener) {
        this.listeners.add(listener);
    }

    public static void main(String[] args) {
        BatteryLevelMonitor monitor = new BatteryLevelMonitor("android_empatica_e4_battery_level");
        monitor.addBatteryLevelListener(new BatteryLevelLogger());
        monitor.monitor(Long.MAX_VALUE);
    }
}

package org.radarcns.process;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.Device;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class BatteryLevelMonitor extends KafkaMonitor {
    private final Set<Device> isLow;
    private final Set<Device> isCritical;
    private final List<BatteryLevelListener> listeners;

    public BatteryLevelMonitor(String topic) {
        super(topic, "radar-test.thehyve.net:9092", "http://radar-test.thehyve.net:8081");

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "battery_monitors");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        this.isLow = new HashSet<>();
        this.isCritical = new HashSet<>();
        this.listeners = new ArrayList<>();
    }

    protected void evaluateRecords(ConsumerRecords<String, GenericRecord> records) {
        for (ConsumerRecord<String, GenericRecord> record : records) {
            Device device = new Device(record.key());
            IndexedRecord value = record.value();
            Schema recordSchema = value.getSchema();

            int batteryLevelFieldId = recordSchema.getField("batteryLevel").pos();
            Number batteryLevel = (Number) value.get(batteryLevelFieldId);
            if (batteryLevel.floatValue() < 0.05) {
                boolean newlyCritical = isCritical.add(device);
                if (newlyCritical) {
                    isLow.add(device);
                    for (BatteryLevelListener listener : listeners) {
                        listener.batteryLevelStatusUpdated(device, BatteryLevelListener.Status.CRITICAL);
                    }
                }
            } else if (batteryLevel.floatValue() < 0.2) {
                if (isLow.add(device)) {
                    for (BatteryLevelListener listener : listeners) {
                        listener.batteryLevelStatusUpdated(device, BatteryLevelListener.Status.LOW);
                    }
                }
            } else {
                if (isLow.remove(device)) {
                    isCritical.remove(device);
                    for (BatteryLevelListener listener : listeners) {
                        listener.batteryLevelStatusUpdated(device, BatteryLevelListener.Status.NORMAL);
                    }
                }
            }
        }
    }

    public void addBatteryLevelListener(BatteryLevelListener listener) {
        this.listeners.add(listener);
    }

    public static void main(String[] args) {
        BatteryLevelMonitor monitor = new BatteryLevelMonitor("empatica_e4_battery_level");
        monitor.addBatteryLevelListener(new BatteryLevelLogger());
        monitor.monitor();
    }
}

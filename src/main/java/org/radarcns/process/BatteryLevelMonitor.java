package org.radarcns.process;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.radarcns.Device;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class BatteryLevelMonitor {
    private final String topic;
    private final static Logger logger = LoggerFactory.getLogger(BatteryLevelMonitor.class);

    private final Set<Device> isLow;
    private final Set<Device> isCritical;
    private final List<BatteryLevelListener> listeners;
    private final KafkaConsumer consumer;

    public BatteryLevelMonitor(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("schema.registry.url", "http://radar-test.thehyve.net:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "battery_monitors");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put("bootstrap.servers", "radar-test.thehyve.net:9092");
        this.topic = topic;

        consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Collections.singletonList(topic));

        this.isLow = new HashSet<>();
        this.isCritical = new HashSet<>();
        this.listeners = new ArrayList<>();
    }

    public void monitor() {
        logger.info("Monitoring stream {}", topic);

        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Long.MAX_VALUE);
            logger.info("Received {} records", records.count());
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

            consumer.commitSync();
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

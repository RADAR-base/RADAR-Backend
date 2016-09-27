package org.radarcns.process;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.radarcns.collect.MockDevice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;

public class BatteryLevelMonitor {
    private final ConsumerConnector consumer;
    private final KafkaAvroDecoder valueDecoder;
    private final KafkaAvroDecoder keyDecoder;
    private final String topic;
    private final KafkaStream<Object, Object> stream;
    private final static Logger logger = LoggerFactory.getLogger(MockDevice.class);

    private final Set<String> isLow;
    private final Set<String> isCritical;

    public BatteryLevelMonitor(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://radar-test.thehyve.net:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "1");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "battery_monitor");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
        props.put("zookeeper.connect", "radar-test.thehyve.net:2181");
        this.topic = topic;

        VerifiableProperties vProps = new VerifiableProperties(props);
        keyDecoder = new KafkaAvroDecoder(vProps);
        valueDecoder = new KafkaAvroDecoder(vProps);

        this.consumer = Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(props));
        HashMap<String, Integer> topicCountMap = new HashMap<>(2);
        topicCountMap.put(topic, 1);
        this.stream = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder).get(topic).get(0);
        this.isLow = new HashSet<>();
        this.isCritical = new HashSet<>();
    }

    public void monitor() {
        logger.info("Monitoring stream {}", topic);
        ConsumerIterator it = stream.iterator();

        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();
            try {
                String key = (String) messageAndMetadata.key();
                IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
                Schema recordSchema = value.getSchema();

                int batteryLevelFieldId = recordSchema.getField("batteryLevel").pos();
                Number batteryLevel = (Number)value.get(batteryLevelFieldId);
                if (batteryLevel.floatValue() < 0.05) {
                    boolean newlyCritical = isCritical.add(key);
                    if (newlyCritical) {
                        isLow.add(key);
                        logger.error("Battery level of device {} is critically low", key);
                    }
                } else if (batteryLevel.floatValue() < 0.2) {
                    if (isLow.add(key)) {
                        logger.warn("Battery level of device {} is low", key);
                    }
                } else {
                    if (isLow.remove(key)) {
                        isCritical.remove(key);
                    }
                }
            } catch (SerializationException e) {
                // may need to do something with it
            }
        }
    }

    public static void main(String[] args) {
        new BatteryLevelMonitor("empatica_e4_battery_level").monitor();
    }
}

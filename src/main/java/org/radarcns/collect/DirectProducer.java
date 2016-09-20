package org.radarcns.collect;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** Directly sends a message to Kafka using a KafkaProducer */
public class DirectProducer<K,V> implements KafkaSender<K, V> {
    private final static Logger logger = LoggerFactory.getLogger(DirectProducer.class);
    private final KafkaProducer<K, V> producer;

    public DirectProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://ubuntu:8081");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void send(String topic, K key, V value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public static void main(String[] args) {
        int numberOfDevices = 1;
        if (args.length > 0) {
            numberOfDevices = Integer.parseInt(args[0]);
        }

        logger.info("Simulating the load of " + numberOfDevices);
        Thread[] threads = new Thread[numberOfDevices];
        for (int i = 0; i < numberOfDevices; i++) {
            threads[i] = new MockDevice(new DirectProducer<>(), "device" + i);
            threads[i].start();
        }
    }
}

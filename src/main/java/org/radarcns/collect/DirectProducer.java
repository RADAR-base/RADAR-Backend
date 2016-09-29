package org.radarcns.collect;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.radarcns.SchemaRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Directly sends a message to Kafka using a KafkaProducer
 */
public class DirectProducer<K, V> implements KafkaSender<K, V> {
    private final static Logger logger = LoggerFactory.getLogger(DirectProducer.class);
    private final KafkaProducer<K, V> producer;
    private long currentOffset;
    private final Map<String, Long> offsetsSent;

    public DirectProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://ubuntu:8081");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
        producer = new KafkaProducer<>(props);
        this.currentOffset = 0L;
        this.offsetsSent = new HashMap<>();
    }

    @Override
    public long send(String topic, K key, V value) {
        producer.send(new ProducerRecord<>(topic, key, value));

        long offset = currentOffset++;
        offsetsSent.put(topic, offset);
        return offset;
    }

    @Override
    public long getLastSentOffset(String topic) {
        return offsetsSent.get(topic);
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        int numberOfDevices = 1;
        if (args.length > 0) {
            numberOfDevices = Integer.parseInt(args[0]);
        }

        logger.info("Simulating the load of " + numberOfDevices);
        MockDevice[] threads = new MockDevice[numberOfDevices];
        KafkaSender<String, GenericRecord>[] senders = new KafkaSender[numberOfDevices];
        SchemaRetriever schemaRetriever = new LocalSchemaRetriever();
        for (int i = 0; i < numberOfDevices; i++) {
            senders[i] = new DirectProducer<>();
            threads[i] = new MockDevice(senders[i], "device" + i, schemaRetriever);
            threads[i].start();
        }
        for (MockDevice device : threads) {
            device.waitFor();
        }
        for (KafkaSender<String, GenericRecord> sender : senders) {
            sender.close();
        }
    }
}

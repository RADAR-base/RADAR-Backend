package org.radarcns.collect;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;

public class AvroConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://ubuntu:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
        props.put("zookeeper.connect", "ubuntu:2181");
        String topic = "test";
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);

        VerifiableProperties vProps = new VerifiableProperties(props);
        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(props));

        Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
                topicCountMap, keyDecoder, valueDecoder);
        int streamNumber = 0;
        for (KafkaStream stream : consumerMap.get(topic)) {
            System.out.println("Processing stream " + streamNumber);
            ConsumerIterator it = stream.iterator();

            while (it.hasNext()) {
                MessageAndMetadata messageAndMetadata = it.next();
                try {
                    String key = (String) messageAndMetadata.key();
                    IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
                    Schema recordSchema = value.getSchema();
                    if (key != null) {
                        System.out.print(key + " -> {");
                    } else {
                        System.out.print("{");
                    }

                    int i = 0;
                    for (Schema.Field field : recordSchema.getFields()) {
                        if (i > 0) {
                            System.out.print(", ");
                        }
                        i++;
                        System.out.print(field.name() + ": " + value.get(field.pos()));
                    }
                    System.out.println("}");
                } catch (SerializationException e) {
                    // may need to do something with it
                }
            }
            streamNumber++;
        }
    }
}

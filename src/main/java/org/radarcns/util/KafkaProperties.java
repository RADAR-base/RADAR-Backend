package org.radarcns.util;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public final class KafkaProperties {
    private static RadarConfig config = RadarConfig.load(KafkaProperties.class.getClassLoader());

    private KafkaProperties() {
        // utility class only
    }

    /**
     * Generate Kafka Producer properties
     */
    public static Properties getSimpleProducer() {
        Properties props = new Properties();

        config.updateProperties(props, CLIENT_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG,
                SCHEMA_REGISTRY_URL_CONFIG);

        props.put(ACKS_CONFIG, "all");
        props.put(RETRIES_CONFIG, 0);
        props.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        return props;
    }

    /**
     * Private method to generate properties for a common Kafka Consumer
     */
    private static Properties getConsumer(String clientId) {
        Properties props = new Properties();

        config.updateProperties(props, CLIENT_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG,
                SCHEMA_REGISTRY_URL_CONFIG, GROUP_ID_CONFIG, SESSION_TIMEOUT_MS_CONFIG);

        if (clientId != null && !clientId.isEmpty()) {
            props.put(CLIENT_ID_CONFIG, clientId);
        }
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("specific.serde.reader", true);

        return props;
    }

    /**
     * Return properties for a Kafka Consumer that manages commits by itself
     */
    public static Properties getSelfCommitConsumer(String clientId) {
        Properties props = getConsumer(clientId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}

package radar.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class KafkaProperties {

    private static RadarConfig config = new RadarConfig();

    /**
     * Generate Kafka Producer properties
     */
    public static Properties getSimpleProducer(){
        Properties props = new Properties();

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getClientID());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaListener());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", KafkaAvroSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", config.getSchemaRegistryURL());

        return props;
    }

    /**
     * Private method to generate properties for a common Kafka Consumer
     */
    private static Properties getConsumer(boolean avroReader){
        Properties props = new Properties();

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, config.getClientID());
        props.put("zookeeper.connect", config.getZooKeeperURL());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaListener());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupID());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.getSessionTimeout());
        props.put("schema.registry.url",config.getSchemaRegistryURL());

        if(avroReader) {
            props.put("specific.avro.reader", true);
        }

        return props;
    }

    /**
     * Return properties for a common Kafka Consumer
     */
    public static Properties getStandardConsumer(boolean avroReader){
        Properties props = getConsumer(avroReader);
        return props;
    }

    /**
     * Return properties for a Kafka Consumer that manages commits by itself
     */
    public static Properties getSelfCommitConsumer(boolean avroReader){
        Properties props = getConsumer(avroReader);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    /**
     * Return properties for a Kafka Consumer with auto commit functionality
     */
    public static Properties getAutoCommitConsumer(boolean avroReader){
        Properties props = getConsumer(avroReader);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitInterval());
        return props;
    }

    /**
     * Return properties for a Kafka Consumer that manages commits by itself and specifies the
     * strategy for partition assignment and offset reset policy
     */
    public static Properties getSelfCommitGroupConsumer(boolean avroReader){
        Properties props = getSelfCommitConsumer(avroReader);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "roundrobin");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");

        return props;
    }

    /**
     * Return properties for a Avro Decoder
     */
    public static Properties getAvroDecoder() {
        Properties props = new Properties();

        props.put("zookeeper.connect", config.getZooKeeperURL());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupID());
        props.put("schema.registry.url", config.getSchemaRegistryURL());

        return props;
    }
}

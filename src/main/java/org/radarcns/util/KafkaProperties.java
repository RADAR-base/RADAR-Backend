package org.radarcns.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.radarcns.util.serde.SpecificAvroSerde;

import java.util.Properties;

import javax.annotation.Nonnull;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
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
    private static Properties getConsumer(boolean avroReader, String clientID){
        Properties props = new Properties();

        clientID = (clientID == null)||(clientID.length() < 0) ? config.getClientID() : clientID;

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientID);
        props.put("zookeeper.connect", config.getZooKeeperURL());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaListener());
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupID());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.getSessionTimeout());
        props.put("schema.registry.url",config.getSchemaRegistryURL());

        if(avroReader) {
            props.put("specific.serde.reader", true);
        }

        return props;
    }

    /**
     * Return properties for a common Kafka Consumer
     */
    public static Properties getStandardConsumer(boolean avroReader){
        Properties props = getConsumer(avroReader,null);
        return props;
    }
    public static Properties getStandardConsumer(boolean avroReader,String clientID){
        Properties props = getConsumer(avroReader,clientID);
        return props;
    }

    /**
     * Return properties for a Kafka Consumer that manages commits by itself
     */
    public static Properties getSelfCommitConsumer(boolean avroReader){
        Properties props = getConsumer(avroReader,null);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
    public static Properties getSelfCommitConsumer(boolean avroReader,String clientID){
        Properties props = getConsumer(avroReader,clientID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    /**
     * Return properties for a Kafka Consumer with auto commit functionality
     */
    public static Properties getAutoCommitConsumer(boolean avroReader){
        Properties props = getConsumer(avroReader,null);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitInterval());
        return props;
    }
    public static Properties getAutoCommitConsumer(boolean avroReader,String clientID){
        Properties props = getConsumer(avroReader,clientID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitInterval());
        return props;
    }

    /**
     * Return properties for a Kafka Consumer that manages commits by itself and specifies the
     * strategy for partition assignment and offset reset policy
     */
    public static Properties getSelfCommitGroupConsumer(boolean avroReader){
        Properties props = getSelfCommitConsumer(avroReader,null);

        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "roundrobin");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");

        return props;
    }
    public static Properties getSelfCommitGroupConsumer(boolean avroReader,String clientID){
        Properties props = getSelfCommitConsumer(avroReader,clientID);

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

    /**
     * Return properties for a Stream
     */
    public static Properties getStream(@Nonnull String clientID, @Nonnull int numThread) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, clientID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaListener());
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, config.getZooKeeperURL());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThread);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    public static Properties getStream(@Nonnull String clientID, @Nonnull int numThread, @Nonnull Class<? extends TimestampExtractor> timestampExtractor) {
        Properties props = getStream(clientID,numThread);

        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, timestampExtractor.getName());

        return props;
    }


    private static Properties getSerdeConsumer(String clientID, String groupID){
        Properties props = new Properties();

        clientID = (clientID == null)||(clientID.length() < 0) ? config.getClientID() : clientID;
        groupID = (groupID == null)||(groupID.length() < 0) ? config.getGroupID() : groupID;

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientID);
        props.put("zookeeper.connect", config.getZooKeeperURL());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaListener());

        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.getSessionTimeout());
        props.put("schema.registry.url",config.getSchemaRegistryURL());

        return props;
    }

    public static Properties getAutoCommitSerdeConsumer(){
        Properties props = getSerdeConsumer(null,null);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitInterval());
        return props;
    }
    public static Properties getAutoCommitSerdeConsumer(String clientID, String groupID){
        Properties props = getSerdeConsumer(clientID,groupID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitInterval());
        return props;
    }

    public static Properties getSelfCommitSerdeConsumer(){
        Properties props = getSerdeConsumer(null,null);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitInterval());
        return props;
    }
    public static Properties getSelfCommitSerdeConsumer(String clientID, String groupID){
        Properties props = getSerdeConsumer(clientID,groupID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getAutoCommitInterval());
        return props;
    }

    public static Properties getSelfCommitSerdeGroupConsumer(){
        Properties props = getSelfCommitSerdeConsumer(null,null);
        return props;
    }
    public static Properties getSelfCommitSerdeGroupConsumer(String clientID, String groupID){
        Properties props = getSelfCommitSerdeConsumer(clientID,groupID);
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "roundrobin");
        return props;
    }

    public static Properties getAutoCommitSerdeGroupConsumer(){
        Properties props = getAutoCommitSerdeConsumer(null,null);
        return props;
    }
    public static Properties getAutoCommitSerdeGroupConsumer(String clientID, String groupID){
        Properties props = getAutoCommitSerdeConsumer(clientID,groupID);
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "roundrobin");
        return props;
    }
}

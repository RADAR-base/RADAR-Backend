package org.radarcns.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.serde.SpecificAvroSerde;

import java.util.Properties;

import javax.annotation.Nonnull;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

// TODO this class should substitute org.radarcns.util.RadarConfig
public class KafkaProperty {

    private ConfigRadar configRadar;
    private KafkaProperty() {}
    protected KafkaProperty(ConfigRadar configRadar) {
        this.configRadar =configRadar;
    }
    /**
     * @param clientID: useful for debugging
     * @param numThread: number of threads to execute stream processing
     * @return Properties for a Kafka Stream
     */
    public Properties getStream(@Nonnull String clientID, @Nonnull int numThread) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, clientID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configRadar.getBrokerPath());
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, configRadar.getZookeeperPath());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, configRadar.getSchemaRegistryPath());
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThread);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    /**
     * @param clientID: useful for debugging
     * @param numThread: number of threads to execute stream processing
     * @param timestampExtractor: custom timestamp extract that overrides the out-of-the-box
     * @return Properties for a Kafka Stream
     */
    public Properties getStream(@Nonnull String clientID, @Nonnull int numThread, @Nonnull Class<? extends TimestampExtractor> timestampExtractor) {
        Properties props = getStream(clientID,numThread);

        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, timestampExtractor.getName());

        return props;
    }

}

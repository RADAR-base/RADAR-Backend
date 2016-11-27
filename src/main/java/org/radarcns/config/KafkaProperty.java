package org.radarcns.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.radarcns.util.serde.SpecificAvroSerde;

import java.util.Properties;

import javax.annotation.Nonnull;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Created by Francesco Nobilia on 27/11/2016.
 */
public class KafkaProperty {

    /**
     * Return properties for a Stream
     */
    public static Properties getStream(@Nonnull String clientID, @Nonnull int numThread) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, clientID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesRadar.getInstance().getBrokerPath());
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, PropertiesRadar.getInstance().getZookeeperPath());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, PropertiesRadar.getInstance().getSchemaRegistryPath());
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

}

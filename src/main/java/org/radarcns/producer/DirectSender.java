package org.radarcns.producer;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.topic.AvroTopic;

/**
 * Directly sends a message to Kafka using a KafkaProducer
 */
public class DirectSender<K, V> implements KafkaSender<K, V> {
    private final KafkaProducer<K, V> producer;

    public DirectSender(RadarPropertyHandler properties) {
        Properties props = new Properties();
        props.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        ConfigRadar radar = properties.getRadarProperties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, radar.getSchemaRegistryPaths());
        props.put(BOOTSTRAP_SERVERS_CONFIG, radar.getBrokerPaths());

        producer = new KafkaProducer<>(props);
    }

    @Override
    public <L extends K, W extends V> KafkaTopicSender<L, W> sender(final AvroTopic<L, W> topic)
            throws IOException {
        return new DirectTopicSender<>(topic);
    }

    @Override
    public boolean resetConnection() {
        return true;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private class DirectTopicSender<L extends K, W extends V> implements KafkaTopicSender<L, W> {
        private long lastOffset = -1L;
        private final AvroTopic<L, W> topic;

        private DirectTopicSender(AvroTopic<L, W> topic) {
            this.topic = topic;
        }

        @Override
        public void send(long offset, L key, W value) throws IOException {
            producer.send(new ProducerRecord<>(topic.getName(), (K)key, (V)value));

            lastOffset = offset;
        }

        @Override
        public void send(List<Record<L, W>> records) throws IOException {
            for (Record<L, W> record : records) {
                producer.send(new ProducerRecord<>(topic.getName(), record.key, record.value));
            }
            lastOffset = records.get(records.size() - 1).offset;
        }

        @Override
        public long getLastSentOffset() {
            return lastOffset;
        }

        @Override
        public void clear() {
            // noop
        }

        @Override
        public void flush() throws IOException {
            producer.flush();
        }

        @Override
        public void close() throws IOException {
            producer.flush();
        }
    }
}

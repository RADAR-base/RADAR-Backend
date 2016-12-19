package org.radarcns.util.serde;

/**
 * Created by Francesco Nobilia on 12/10/2016.
 */

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class GenericAvroSerializer implements Serializer<GenericRecord> {

    private final KafkaAvroSerializer inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public GenericAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, GenericRecord record) {
        return inner.serialize(topic, record);
    }

    @Override
    public void close() {
        inner.close();
    }
}

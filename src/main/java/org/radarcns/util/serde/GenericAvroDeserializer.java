package org.radarcns.util.serde;

/**
 * Created by Francesco Nobilia on 12/10/2016.
 */

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GenericAvroDeserializer implements Deserializer<GenericRecord> {

    private final KafkaAvroDeserializer inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public GenericAvroDeserializer() {
        inner = new KafkaAvroDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public GenericRecord deserialize(String s, byte[] bytes) {
        return (GenericRecord) inner.deserialize(s, bytes);
    }

    @Override
    public void close() {
        inner.close();
    }
}

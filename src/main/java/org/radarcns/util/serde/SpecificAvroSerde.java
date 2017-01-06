package org.radarcns.util.serde;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SpecificAvroSerde<T extends SpecificRecord> implements Serde<T> {
    private final Serde<T> inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public SpecificAvroSerde() {
        inner = Serdes.serdeFrom(new SpecificAvroSerializer<>(), new SpecificAvroDeserializer<>());
    }

    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}
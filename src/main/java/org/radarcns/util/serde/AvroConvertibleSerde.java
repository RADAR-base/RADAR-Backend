package org.radarcns.util.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.radarcns.util.SpecificAvroConvertible;

public class AvroConvertibleSerde<T extends SpecificAvroConvertible> implements Serde<T> {
    private final Supplier<T> supplier;
    private final SpecificAvroSerde<SpecificRecord> subSerde;

    AvroConvertibleSerde(Supplier<T> supplier, SchemaRegistryClient schemaRegistryClient) {
        this.supplier = supplier;
        subSerde = new SpecificAvroSerde<>(schemaRegistryClient);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        subSerde.configure(configs, isKey);
    }

    @Override
    public void close() {
        subSerde.close();
    }

    @Override
    public Serializer<T> serializer() {
        final Serializer<SpecificRecord> subSerializer = subSerde.serializer();
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                subSerializer.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(String topic, T data) {
                SpecificRecord record = data.toAvro();
                return subSerializer.serialize(topic, record);
            }

            @Override
            public void close() {
                subSerializer.close();
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        final Deserializer<SpecificRecord> subDeserializer = subSerde.deserializer();
        final Supplier<T> localSupplier = supplier;
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                subDeserializer.configure(configs, isKey);
            }

            @Override
            public T deserialize(String topic, byte[] data) {
                T result = localSupplier.get();
                result.fromAvro(subDeserializer.deserialize(topic, data));
                return result;
            }

            @Override
            public void close() {
                subDeserializer.close();
            }
        };
    }
}

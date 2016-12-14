package org.radarcns.util.serde;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

/**
 * It generates the jsonSerializer and jsonDeserializer for the given input class
 */
public class RadarSerde<T> {

    private JsonSerializer<T> jsonSerializer;
    private JsonDeserializer<T> jsonDeserializer;

    public RadarSerde(Class<T> type) {
        this.jsonSerializer = new JsonSerializer<>();
        this.jsonDeserializer = new JsonDeserializer<>(type);
    }

    public Serde<T> getSerde(){
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public Serde<Windowed<T>> getWindowed(){
        WindowedSerializer<T> windowedSerializer = new WindowedSerializer<>(jsonSerializer);
        WindowedDeserializer<T> windowedDeserializer = new WindowedDeserializer<>(jsonDeserializer);
        return Serdes.serdeFrom(windowedSerializer,windowedDeserializer);
    }
}

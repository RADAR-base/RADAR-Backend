package org.radarcns.util.serde;

/**
 * Created by Francesco Nobilia on 21/10/2016.
 */

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final static Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);
    private final static ObjectReader reader = new ObjectMapper().reader();
    private final static JsonFactory jsonFactory = new JsonFactory();

    private Class<T> deserializedClass;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        if(deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        try {
            return reader.readValue(jsonFactory.createParser(bytes), deserializedClass);
        } catch (IOException e) {
            logger.error("Failed to deserialize value for topic " + topic);
            return null;
        }
    }

    @Override
    public void close() {

    }
}
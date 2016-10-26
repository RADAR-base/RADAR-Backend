package org.radarcns.util.serde;

/**
 * Created by Francesco Nobilia on 21/10/2016.
 */
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private final static Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    private final static ObjectWriter writer = new ObjectMapper().writer();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, T t) {
        try {
            return writer.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            logger.error("Cannot deserialize value " + t + " in topic " + topic);
            return null;
        }
    }

    @Override
    public void close() {

    }
}

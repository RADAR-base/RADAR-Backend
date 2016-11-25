package org.radarcns.util.serde;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    
    private final static Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    
    private final static ObjectWriter writer = getFieldMapper().writer();

    private static ObjectMapper getFieldMapper() {
        ObjectMapper mapper = new ObjectMapper();

        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        return mapper;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, T t) {
        try {
            return writer.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            logger.error("Cannot serialize value {} in topic {}", t, topic, e);
            return null;
        }
    }

    @Override
    public void close() {

    }
}

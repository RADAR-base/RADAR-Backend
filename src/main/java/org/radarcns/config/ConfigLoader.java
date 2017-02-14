package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;

public class ConfigLoader {
    private final ObjectMapper mapper;

    public ConfigLoader() {
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        // only serialize fields, not getters, etc.
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
    }

    public <T> T load(File file, Class<T> configClass) throws IOException {
        return mapper.readValue(file, configClass);
    }

    public void store(File file, Object config) throws IOException {
        mapper.writeValue(file, config);
    }


    public String prettyString(Object config) {
        // pretty print
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // make ConfigRadar the root element
        mapper.enable(SerializationFeature.WRAP_ROOT_VALUE);

        try {
            return mapper.writeValueAsString(config);
        } catch (JsonProcessingException ex) {
            throw new UnsupportedOperationException("Cannot serialize config", ex);
        } finally {
            mapper.disable(SerializationFeature.INDENT_OUTPUT);
            mapper.disable(SerializationFeature.WRAP_ROOT_VALUE);
        }
    }
}

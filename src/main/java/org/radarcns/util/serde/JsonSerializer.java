/*
 * Copyright 2017 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    
    private static final ObjectWriter WRITER = getFieldMapper().writer();

    private static ObjectMapper getFieldMapper() {
        ObjectMapper mapper = new ObjectMapper();

        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        return mapper;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // no configuration needed
    }

    @Override
    public byte[] serialize(String topic, T t) {
        try {
            return WRITER.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            logger.error("Cannot serialize value {} in topic {}", t, topic, e);
            return null;
        }
    }

    @Override
    public void close() {
        // noop
    }
}

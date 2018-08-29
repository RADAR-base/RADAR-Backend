/*
 * Copyright 2017 King's College London and The Hyve
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

import static org.radarcns.util.serde.RadarSerde.GENERIC_WRITER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSerializer<T> implements Serializer<T> {
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);

    private final ObjectWriter writer;

    public JsonSerializer() {
        this.writer = GENERIC_WRITER;
    }

    public JsonSerializer(Class<T> cls) {
        this.writer = GENERIC_WRITER.forType(cls);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // no configuration needed
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
        // noop
    }
}

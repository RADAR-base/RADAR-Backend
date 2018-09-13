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

import static org.radarcns.util.serde.RadarSerde.GENERIC_READER;

import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonDeserializer<T> implements Deserializer<T> {
    private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);

    private final ObjectReader reader;

    public JsonDeserializer(Class<T> deserializedClass) {
        reader = GENERIC_READER.forType(deserializedClass);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        // no configuration
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            return reader.readValue(bytes);
        } catch (IOException e) {
            logger.error("Failed to deserialize value for topic {}", topic, e);
            return null;
        }
    }

    @Override
    public void close() {
        // noop
    }
}
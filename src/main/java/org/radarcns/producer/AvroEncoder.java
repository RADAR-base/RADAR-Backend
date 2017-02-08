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

package org.radarcns.producer;

import org.apache.avro.Schema;

import java.io.IOException;

/** Encode Avro values with a given encoder */
public interface AvroEncoder {
    /** Create a new writer. This method is thread-safe, but the class it returns is not. */
    <T> AvroWriter<T> writer(Schema schema, Class<T> clazz) throws IOException;

    interface AvroWriter<T> {
        /** Encode an object. This method is not thread-safe. */
        byte[] encode(T object) throws IOException;
    }
}

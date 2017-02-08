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

/**
 * Parsed schema metadata from a Schema Registry.
 */
public class ParsedSchemaMetadata {
    private final Integer version;
    private Integer id;
    private final Schema schema;

    public ParsedSchemaMetadata(Integer id, Integer version, Schema schema) {
        this.id = id;
        this.version = version;
        this.schema = schema;
    }

    public Integer getId() {
        return id;
    }

    public Schema getSchema() {
        return schema;
    }

    public Integer getVersion() {
        return version;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}

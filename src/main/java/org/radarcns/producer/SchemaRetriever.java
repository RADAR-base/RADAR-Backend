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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/** Retriever of an Avro Schema */
public class SchemaRetriever {
    private static final Logger logger = LoggerFactory.getLogger(SchemaRetriever.class);
    private final ConcurrentMap<String, ParsedSchemaMetadata> cache;
    private final CachedSchemaRegistryClient schemaClient;

    public SchemaRetriever(String url) {
        cache = new ConcurrentHashMap<>();
        this.schemaClient = new CachedSchemaRegistryClient(url, 1024);
    }

    /** The subject in the Avro Schema Registry, given a Kafka topic. */
    protected String subject(String topic, boolean ofValue) {
        return topic + (ofValue ? "-value" : "-key");
    }

    /** Retrieve schema metadata */
    protected ParsedSchemaMetadata retrieveSchemaMetadata(String topic, boolean ofValue)
            throws IOException {
        String subject = subject(topic, ofValue);

        try {
            SchemaMetadata metadata = schemaClient.getLatestSchemaMetadata(subject);
            Schema schema = parseSchema(metadata.getSchema());
            return new ParsedSchemaMetadata(metadata.getId(), metadata.getVersion(), schema);
        } catch (RestClientException ex) {
            throw new IOException(ex);
        }
    }

    public ParsedSchemaMetadata getSchemaMetadata(String topic, boolean ofValue)
            throws IOException {
        ParsedSchemaMetadata value = cache.get(subject(topic, ofValue));
        if (value == null) {
            value = retrieveSchemaMetadata(topic, ofValue);
            ParsedSchemaMetadata oldValue = cache.putIfAbsent(subject(topic, ofValue), value);
            if (oldValue != null) {
                value = oldValue;
            }
        }
        return value;
    }

    /** Parse a schema from string. */
    protected Schema parseSchema(String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    /**
     * Add schema metadata to the retriever.
     *
     * This implementation only adds it to the cache.
     */
    public void addSchemaMetadata(String topic, boolean ofValue, ParsedSchemaMetadata metadata)
            throws IOException {
        if (metadata.getId() == null) {
            try {
                int id = schemaClient.register(subject(topic, ofValue), metadata.getSchema());
                metadata.setId(id);
            } catch (RestClientException ex) {
                throw new IOException(ex);
            }
        }
        cache.put(subject(topic, ofValue), metadata);
    }

    /**
     * Get schema metadata, and if none is found, add a new schema.
     */
    public ParsedSchemaMetadata getOrSetSchemaMetadata(String topic, boolean ofValue, Schema schema)
            throws IOException {
        ParsedSchemaMetadata metadata;
        try {
            metadata = getSchemaMetadata(topic, ofValue);
            if (metadata.getSchema().equals(schema)) {
                return metadata;
            }
        } catch (IOException ex) {
            logger.warn("Schema for {} value was not yet added to the schema registry.", topic);
        }

        metadata = new ParsedSchemaMetadata(null, null, schema);
        addSchemaMetadata(topic, ofValue, metadata);
        return metadata;
    }
}

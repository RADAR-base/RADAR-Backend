package org.radarcns;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class SchemaRetriever {
    private final static Logger logger = LoggerFactory.getLogger(SchemaRetriever.class);
    private final Map<String, ParsedSchemaMetadata> cache;

    public SchemaRetriever() {
        cache = new HashMap<>();
    }

    protected String subject(String topic, boolean ofValue) {
        return topic + (ofValue ? "-value" : "-key");
    }

    protected abstract ParsedSchemaMetadata retrieveSchemaMetadata(String topic, boolean ofValue) throws IOException;

    public ParsedSchemaMetadata getSchemaMetadata(String topic, boolean ofValue) throws IOException {
        ParsedSchemaMetadata value = cache.get(subject(topic, ofValue));
        if (value == null) {
            value = retrieveSchemaMetadata(topic, ofValue);
            cache.put(subject(topic, ofValue), value);
        }
        return value;
    }

    protected Schema parseSchema(String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    public void addSchemaMetadata(String topic, boolean ofValue, ParsedSchemaMetadata metadata) throws IOException {
        cache.put(subject(topic, ofValue), metadata);
    }

    public ParsedSchemaMetadata getOrSetSchemaMetadata(String topic, boolean ofValue, Schema schema) {
        try {
            return getSchemaMetadata(topic, ofValue);
        } catch (IOException ex) {
            logger.debug("Schema for {} value was not yet added.", topic);
        }

        ParsedSchemaMetadata metadata = new ParsedSchemaMetadata(null, null, schema);
        try {
            addSchemaMetadata(topic, ofValue, metadata);
        } catch (IOException ex) {
            logger.error("Failed to add schema for {} value", topic, ex);
        }
        return metadata;
    }
}

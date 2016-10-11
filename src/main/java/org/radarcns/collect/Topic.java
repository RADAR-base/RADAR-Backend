package org.radarcns.collect;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.radarcns.SchemaRetriever;

import java.io.IOException;


public class Topic {
    private final String name;
    private final Schema schema;
    private final static Schema keySchema = Schema.create(Schema.Type.STRING);

    public Topic(String name, SchemaRetriever retriever) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("Name may not be null");
        }
        this.name = name;
        this.schema = retriever.getSchemaMetadata(getName(), true).getSchema();
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public Schema getValueSchema() {
        return schema;
    }

    public GenericRecord createSimpleRecord(double time, Object... values) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        if (schema.getField("time") != null) {
            avroRecord.put("time", time);
        }
        if (schema.getField("timeReceived") != null) {
            avroRecord.put("timeReceived", System.currentTimeMillis() / 1000.0);
        }
        for (int i = 0; i < values.length; i += 2) {
            avroRecord.put((String) values[i], values[i + 1]);
        }
        return avroRecord;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Topic topic = (Topic) o;

        return name.equals(topic.name) && schema.equals(topic.schema);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}

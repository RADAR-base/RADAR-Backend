package org.radarcns.collect;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.radarcns.SchemaRetriever;

import java.io.IOException;


public class Topic {
    private final String name;
    private Schema schema;

    public Topic(String name, SchemaRetriever retriever) throws IOException {
        this.name = name;
        this.schema = retriever.getSchemaMetadata(getName(), true).getSchema();
    }

    public Schema getValueSchema() throws IOException {
        return schema;
    }

    public GenericRecord createSimpleRecord(Object... values) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        if (schema.getField("time") != null) {
            avroRecord.put("time", System.currentTimeMillis() / 1000.0);
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

        return name != null ? name.equals(topic.name) : topic.name == null;

    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}

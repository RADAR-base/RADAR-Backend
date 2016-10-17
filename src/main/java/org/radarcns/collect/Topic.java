package org.radarcns.collect;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.radarcns.SchemaRetriever;

import java.io.IOException;
import java.util.List;


public class Topic {
    private final String name;
    private final Schema schema;
    private final static Schema keySchema = Schema.create(Schema.Type.STRING);
    private final Schema.Field timeField;
    private final Schema.Field timeReceivedField;

    public Topic(String name, SchemaRetriever retriever) throws IOException {
        if (name == null) {
            throw new IllegalArgumentException("Name may not be null");
        }
        this.name = name;
        this.schema = retriever.getSchemaMetadata(getName(), true).getSchema();
        this.timeField = schema.getField("time");
        this.timeReceivedField = schema.getField("timeReceived");
        if (timeField == null) {
            throw new IllegalArgumentException("Schema must have time as its first field");
        }
        if (timeReceivedField == null) {
            throw new IllegalArgumentException("Schema must have timeReceived as its second field");
        }
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public Schema getValueSchema() {
        return schema;
    }

    public Schema.Field getValueField(String name) {
        Schema.Field field = schema.getField(name);
        if (field == null) {
            throw new IllegalArgumentException("Field " + name + " not in value schema");
        }
        return field;
    }

    public GenericRecord createSimpleRecord(double time, Object... values) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put(timeField.pos(), time);
        avroRecord.put(timeReceivedField.pos(), System.currentTimeMillis() / 1000.0);
        for (int i = 0; i < values.length; i += 2) {
            if (values[i] instanceof Schema.Field) {
                avroRecord.put(((Schema.Field) values[i]).pos(), values[i + 1]);
            } else if (values[i] instanceof String) {
                avroRecord.put((String) values[i], values[i + 1]);
            } else {
                throw new IllegalArgumentException("Record key " + values[i] + " is not a Schema.Field or String");
            }
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

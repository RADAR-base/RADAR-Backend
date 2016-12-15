package org.radarcns.sink.hdfs;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.avro.AvroRecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.confluent.connect.avro.AvroData.ANYTHING_SCHEMA;

/**
 * Writes data to HDFS using the Confluent Kafka HDFS connector.
 *
 * This will write both the keys and values of the data to file, in a new Avro format with as its two fields the "key"
 * and "value" fields, with the original Avro schema of the key and value as the type of those fields.
 *
 * To use, implement a {@link Format#getRecordWriterProvider()} that returns this class (i.e., {@link AvroFormatRadar},
 * and provide that in the `format.class` property.
 */
public class AvroRecordWriterProviderRadar  implements RecordWriterProvider {

    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
    private static final String EXTENSION = ".avro";

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    /**
     * The returned RecordWriter method assumes that all {@link SinkRecord} objects provided to it have the same schema
     * and that the record indeed HAS both a key and value schema.
     */
    @Override
    public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName,
                                                    SinkRecord record, final AvroData avroData)
            throws IOException {
        final Schema keySchema = record.keySchema();
        final Schema valueSchema = record.valueSchema();

        SchemaBuilder.RecordBuilder<org.apache.avro.Schema> builder = SchemaBuilder
                .record(getSchemaName(keySchema) + "_" + getSchemaName(valueSchema))
                .namespace("org.radarcns.combined").doc("combined key-value record");

        final org.apache.avro.Schema combinedSchema = builder.fields()
                .name("key").doc("Key of a Kafka SinkRecord")
                    .type(getSchema(avroData, keySchema)).noDefault()
                .name("value").doc("Value of a Kafka SinkRecord")
                    .type(getSchema(avroData, valueSchema)).noDefault()
                .endRecord();

        Path path = new Path(fileName);
        final FSDataOutputStream out = path.getFileSystem(conf).create(path);

        final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
        writer.create(combinedSchema, out);

        return new RecordWriter<SinkRecord>(){
            @Override
            public void write(SinkRecord record) throws IOException {
                log.trace("Sink record: {}", record.toString());

                GenericRecord combinedRecord = new GenericData.Record(combinedSchema);
                write(combinedRecord, 0, keySchema, record.key());
                write(combinedRecord, 1, valueSchema, record.value());
                writer.append(combinedRecord);
            }

            private void write(GenericRecord record, int index, Schema schema, Object data) {
                if (data == null) {
                    return;
                }
                Object outputData = avroData.fromConnectData(schema, data);
                if (outputData instanceof NonRecordContainer) {
                    outputData = ((NonRecordContainer) outputData).getValue();
                }
                record.put(index, outputData);
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }
        };
    }

    private String getSchemaName(Schema schema) {
        if (schema != null) {
            String schemaName = schema.name();
            if (schemaName != null) {
                return schemaName;
            } else {
                return schema.type().getName();
            }
        } else {
            return null;
        }
    }

    private org.apache.avro.Schema getSchema(AvroData avroData, Schema schema) {
        if (schema == null) {
            return org.apache.avro.Schema.createUnion(
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                    ANYTHING_SCHEMA);
        } else {
            return avroData.fromConnectSchema(schema);
        }
    }
}
package org.radarcns.sink.HDFS;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.avro.AvroRecordWriterProvider;

/**
 * Created by nivethika on 7-11-16.
 */
public class AvroRecordWriterProviderRadar  implements RecordWriterProvider {

    private final String keyFieldName = "keyField";
    private final String valueFieldName = "valueField";

    private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
    private final static String EXTENSION = ".avro";

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName,
                                                    SinkRecord record, final AvroData avroData)
            throws IOException {
        DatumWriter<Object> datumWriter = new GenericDatumWriter<>();
        final DataFileWriter<Object> writer = new DataFileWriter<>(datumWriter);
        Path path = new Path(fileName);


        final Schema valueSchema = record.valueSchema();
        final Schema keySchema = record.keySchema();


        org.apache.avro.Schema avroValueSchema = avroData.fromConnectSchema(valueSchema);
        org.apache.avro.Schema avroKeySchema = avroData.fromConnectSchema(keySchema);
        final String name = avroValueSchema.getName();

        final org.apache.avro.Schema combinedSchema = org.apache.avro.Schema.createRecord("combinedSchema"+name, "combined key value record", "org.radarcns.empaticaE4", false);
        combinedSchema.setFields((Arrays.asList(
                new org.apache.avro.Schema.Field(keyFieldName , avroKeySchema, "keySchema" , null)
                ,new org.apache.avro.Schema.Field(valueFieldName , avroValueSchema, "valueSchema" , null))));


        final FSDataOutputStream out = path.getFileSystem(conf).create(path);

        writer.create(combinedSchema, out);

        return new RecordWriter<SinkRecord>(){
            @Override
            public void write(SinkRecord record) throws IOException {
                log.trace("Sink record: {}", record.toString());
                Object value = avroData.fromConnectData(valueSchema, record.value());
                Object key = avroData.fromConnectData(keySchema, record.key());

                if(value instanceof NonRecordContainer)
                {
                    value = ((NonRecordContainer) value).getValue();
                }
                if(key instanceof NonRecordContainer)
                {
                    key = ((NonRecordContainer) key).getValue();
                }

                GenericRecord combinedRecord = new GenericData.Record(combinedSchema);
                combinedRecord.put(keyFieldName , key);
                combinedRecord.put(valueFieldName, value);
                writer.append(combinedRecord);
            }

            @Override
            public void close() throws IOException {
                writer.close();
            }
        };
    }
}
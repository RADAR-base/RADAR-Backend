package org.radarcns.sink.hdfs;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroRecordWriterProviderRadarTest {
    private AvroData avroData;
    private AvroRecordWriterProviderRadar provider;
    private Configuration conf;
    private String outputFile;

    @Before
    public void setUp() throws IOException {
        provider = new AvroRecordWriterProviderRadar();
        outputFile = File.createTempFile("AvroTest", null).getAbsolutePath();
        conf = new Configuration();
        avroData = new AvroData(100);
    }


    @Test
    public void recordWriter() throws Exception {
        SinkRecord record = new SinkRecord("mine", 0, null, null,
                SchemaBuilder.string().build(), "hi", 0);
        RecordWriter<SinkRecord> writer = provider.getRecordWriter(conf, outputFile, record, avroData);
        writer.write(record);
        writer.write(new SinkRecord("mine", 0, null, "withData",
                SchemaBuilder.string().build(), "hi", 0));
        writer.close();
        assertTrue(0 < outputFile.length());
    }

    @Test(expected = DataFileWriter.AppendWriteException.class)
    public void recordWriterWrongSchema() throws Exception {
        SinkRecord record = new SinkRecord("mine", 0, SchemaBuilder.string().build(), "something",
                SchemaBuilder.string().build(), "hi", 0);
        RecordWriter<SinkRecord> writer = provider.getRecordWriter(conf, outputFile, record, avroData);
        writer.write(new SinkRecord("mine", 0, null, null,
                    SchemaBuilder.string().build(), "hi", 0));
        writer.close();
        assertEquals(0, outputFile.length());
    }
}
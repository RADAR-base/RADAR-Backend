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

package org.radarcns.sink.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

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
        RecordWriter<SinkRecord> writer = provider.getRecordWriter(
                conf, outputFile, record, avroData);
        writer.write(record);
        writer.write(new SinkRecord("mine", 0, null, "withData",
                SchemaBuilder.string().build(), "hi", 0));
        writer.close();
        assertTrue(0 < outputFile.length());
    }

    @Test(expected = DataFileWriter.AppendWriteException.class)
    public void recordWriterWrongSchema() throws Exception {
        SinkRecord record = new SinkRecord("mine", 0, SchemaBuilder.string().build(),
                "something", SchemaBuilder.string().build(), "hi", 0);
        RecordWriter<SinkRecord> writer = provider.getRecordWriter(
                conf, outputFile, record, avroData);
        writer.write(new SinkRecord("mine", 0, null, null,
                    SchemaBuilder.string().build(), "hi", 0));
        writer.close();
        assertEquals(0, outputFile.length());
    }
}
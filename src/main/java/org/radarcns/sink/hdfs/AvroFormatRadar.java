package org.radarcns.sink.hdfs;

import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.avro.AvroFormat;

/**
 * Extended AvroFormat class to support custom AvroRecordWriter to allow writting key and value to HDFS
 */
public class AvroFormatRadar extends AvroFormat {

    public RecordWriterProvider getRecordWriterProvider() {
        return new AvroRecordWriterProviderRadar();
    }
}

package org.radarcns.sink.HDFS;

import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.avro.AvroFormat;

/**
 * Created by nivethika on 7-11-16.
 */
public class AvroFormatRadar extends AvroFormat {

    public RecordWriterProvider getRecordWriterProvider() {
        return new AvroRecordWriterProviderRadar();
    }
}

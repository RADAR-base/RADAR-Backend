package org.radarcns.collect.rest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.radarcns.SchemaRetriever;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;

public class GenericRecordEncoder implements AvroEncoder<GenericRecord> {
    private final EncoderFactory encoderFactory;

    public GenericRecordEncoder() {
        this.encoderFactory = EncoderFactory.get();
    }

    public String encode(GenericRecord record, Schema schema) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()){
            Encoder encoder = this.encoderFactory.jsonEncoder(schema, out);
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            writer.write(record, encoder);
            encoder.flush();

            return new String(out.toByteArray());
        }
    }
}

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

package org.radarcns.producer;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** An AvroEncoder to encode known SpecificRecord classes */
public class SpecificRecordEncoder implements AvroEncoder {
    private final EncoderFactory encoderFactory;
    private final boolean binary;

    /**
     * Create a SpecificRecordEncoder.
     * @param binary whether to use binary encoding or JSON.
     */
    public SpecificRecordEncoder(boolean binary) {
        this.encoderFactory = EncoderFactory.get();
        this.binary = binary;
    }

    @Override
    public <T> AvroWriter<T> writer(Schema schema, Class<T> clazz) throws IOException {
        if (!SpecificRecord.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Can only create readers for SpecificRecords.");
        }
        return new AvroRecordWriter<>(encoderFactory, schema, new SpecificDatumWriter<T>(schema));
    }

    private class AvroRecordWriter<T> implements AvroEncoder.AvroWriter<T> {
        private final Encoder encoder;
        private final ByteArrayOutputStream out;
        private final DatumWriter<T> writer;

        private AvroRecordWriter(EncoderFactory encoderFactory, Schema schema,
                DatumWriter<T> writer) throws IOException {
            this.writer = writer;
            out = new ByteArrayOutputStream();
            if (binary) {
                encoder = encoderFactory.binaryEncoder(out, null);
            } else {
                encoder = encoderFactory.jsonEncoder(schema, out);
            }
        }

        public byte[] encode(T record) throws IOException {
            try {
                writer.write(record, encoder);
                encoder.flush();
                return out.toByteArray();
            } finally {
                out.reset();
            }
        }
    }
}

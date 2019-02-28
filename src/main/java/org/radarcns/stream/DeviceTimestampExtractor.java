/*
 * Copyright 2017 King's College London and The Hyve
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

package org.radarcns.stream;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Custom TimestampExtractor for TimeWindows Streams. */
public class DeviceTimestampExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(DeviceTimestampExtractor.class);

    /**
     * Return the timeReceived value converted in long. timeReceived is the timestamp at which the
     * device has collected the sample.
     *
     * @throws IllegalArgumentException if timeReceived is not present inside the analysed record
     */
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        IndexedRecord value = (IndexedRecord) record.value();
        Schema recordSchema = value.getSchema();

        try {
            Schema.Field field = recordSchema.getField("timeReceived");
            if (value.get(field.pos()) instanceof Double) {
                return (long) (1000d * (Double) value.get(field.pos()));
            } else {
                log.error("timeReceived id not a Double in {}", record);
            }

        } catch (AvroRuntimeException e) {
            log.error("Cannot extract timeReceived form {}", record, e);
        }

        throw new IllegalArgumentException("Impossible to extract timeReceived from " + record);
    }
}

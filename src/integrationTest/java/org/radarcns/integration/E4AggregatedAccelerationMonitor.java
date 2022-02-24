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

package org.radarcns.integration;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.monitor.AbstractKafkaMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer for Aggregated Acceleration Stream.
 */
public class E4AggregatedAccelerationMonitor
        extends AbstractKafkaMonitor<GenericRecord, GenericRecord, Object> {
    private static final Logger logger = LoggerFactory.getLogger(
            E4AggregatedAccelerationMonitor.class);

    /** Constructor. */
    public E4AggregatedAccelerationMonitor(RadarPropertyHandler radar, String topic,
            String clientId) {
        super(radar, List.of(topic), "new", clientId, null);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(radar.getRadarProperties().getStream().getProperties());
        configure(props);
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        // noop
    }

    @Override
    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {

            GenericRecord key = record.key();
            if (key == null) {
                logger.error("Failed to process record {} without a key.", record);
                return;
            }
            Schema keySchema = key.getSchema();
            if (keySchema.getField("userId") != null
                    && keySchema.getField("sourceId") != null) {
                assertNotNull(key.get("userId"));
                assertNotNull(key.get("sourceId"));
            } else {
                logger.error("Failed to process record {} with wrong key type {}.",
                        record, key.getSchema());
                return;
            }
            GenericRecord value = record.value();
            GenericData.Array fields = (GenericData.Array) value.get("fields");
            logger.info("Received [{}, {}, {}] E4 messages",
                    ((GenericRecord)fields.get(0)).get("count"),
                    ((GenericRecord)fields.get(1)).get("count"),
                    ((GenericRecord)fields.get(2)).get("count"));

            if ((Integer)((GenericRecord)fields.get(0)).get("count") > 100) {
                shutdown();
            }
        }
    }
}

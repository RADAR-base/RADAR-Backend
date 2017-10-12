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

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Consumer for Aggregated Acceleration Stream
 */
public class E4AggregatedAccelerationMonitor extends AbstractKafkaMonitor<GenericRecord, GenericRecord, Object> {
    private static final Logger logger = LoggerFactory.getLogger(E4AggregatedAccelerationMonitor.class);

    public E4AggregatedAccelerationMonitor(RadarPropertyHandler radar, String topic, String clientID) throws IOException {
        super(radar, Collections.singletonList(topic), "new", clientID, null);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(radar.getRadarProperties().getStreamProperties());
        configure(props);
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> records) {
        // noop
    }

    @Override
    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        if (records.isEmpty()) {
            shutdown();
            return;
        }
        assertTrue(records.count() > 0);
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
            GenericData.Array count = (GenericData.Array) value.get("count");
            logger.info("Received [{}, {}, {}] E4 messages",
                    count.get(0), count.get(1), count.get(2));
        }
    }
}

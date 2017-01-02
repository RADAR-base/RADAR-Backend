package org.radarcns.integration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.radarcns.key.MeasurementKey;
import org.radarcns.process.KafkaMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Consumer for Aggregated Acceleration Stream
 */
public class E4AggregatedAccelerationMonitor extends KafkaMonitor<GenericRecord, GenericRecord> {
    private static final Logger logger = LoggerFactory.getLogger(E4AggregatedAccelerationMonitor.class);
    private boolean isDone =false;

    public E4AggregatedAccelerationMonitor(String topic, String clientID) {
        super(Collections.singletonList(topic),clientID);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "new");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configure(props);
    }

    protected void evaluateRecords(ConsumerRecords<GenericRecord, GenericRecord> records) {
        if(records.count()==0)
        {
            isDone=true;
            return;
        }
        assertTrue(records.count()>0);
        for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {

            GenericRecord key = record.key();
            if (key == null) {
                logger.error("Failed to process record {} without a key.", record);
                return;
            }
            MeasurementKey measurementKey;
            Schema keySchema = key.getSchema();
            if (keySchema.getField("userID") != null
                    && keySchema.getField("sourceID") != null) {
                measurementKey = new MeasurementKey((String) key.get("userID"),
                        (String) key.get("sourceID"));
                assertNotNull(measurementKey);
            } else {
                logger.error("Failed to process record {} with wrong key type {}.",
                        record, key.getSchema());
                return;
            }
            GenericRecord value = record.value();
            Schema recordSchema = value.getSchema();

            int minFieldId = recordSchema.getField("min").pos();
            assertNotNull(minFieldId);

            GenericData.Array min = (GenericData.Array) value.get(minFieldId);
            assertNotNull(min);
            assertEquals((double)min.get(0), 15.0d, 0.0);
            assertEquals((double)min.get(1), -15.0d, 0.0);
            assertEquals((double)min.get(2), 64.0d, 0.0);

            int maxFieldId = recordSchema.getField("max").pos();
            assertNotNull(maxFieldId);

            GenericData.Array max = (GenericData.Array) value.get(maxFieldId);
            assertNotNull(max);
            assertEquals((double)max.get(0), 15.0d, 0.0);
            assertEquals((double)max.get(1), Double.MIN_VALUE, 0.0d);
            assertEquals((double)max.get(2), 64.0d, 0.0);
        }
    }

    protected boolean isDone() {
        return this.isDone;
    }

}

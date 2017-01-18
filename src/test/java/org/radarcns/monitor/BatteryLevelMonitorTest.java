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

package org.radarcns.monitor;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.radarcns.monitor.BatteryLevelMonitor.Status.LOW;

import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.stream.aggregator.DeviceTimestampExtractorTest;
import org.radarcns.util.EmailSender;

public class BatteryLevelMonitorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void evaluateRecord() throws Exception {
        EmailSender sender = mock(EmailSender.class);

        ConfigRadar config = KafkaMonitorFactoryTest
                .getBatteryMonitorConfig(25252, folder);
        RadarPropertyHandler properties = KafkaMonitorFactoryTest
                .getRadarPropertyHandler(config, folder);

        String keySchema = "{\"name\": \"key\", \"type\": \"record\", \"fields\": ["
                + "{\"name\": \"userId\", \"type\": \"string\"},"
                + "{\"name\": \"sourceId\", \"type\": \"string\"}"
                + "]}";

        String valueSchema = "{\"name\": \"value\", \"type\": \"record\", \"fields\": ["
                + "{\"name\": \"timeReceived\", \"type\": \"double\"},"
                + "{\"name\": \"batteryLevel\", \"type\": \"float\"}"
                + "]}";

        BatteryLevelMonitor monitor = new BatteryLevelMonitor(properties,
                Collections.singletonList("mytopic"), sender, LOW);

        GenericRecord key = DeviceTimestampExtractorTest.buildIndexedRecord(keySchema);
        key.put("sourceId", "1");
        key.put("userId", "me");
        GenericRecord value = DeviceTimestampExtractorTest.buildIndexedRecord(valueSchema);
        ConsumerRecord<GenericRecord, GenericRecord> record = new ConsumerRecord<>(
                "mytopic", 0, 1000L, key, value);

        value.put("timeReceived", 2000L);
        value.put("batteryLevel", 1.0f);
        monitor.evaluateRecord(record);
        verify(sender, never()).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2001L);
        value.put("batteryLevel", 0.1f);
        monitor.evaluateRecord(record);
        verify(sender, times(1)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2002L);
        value.put("batteryLevel", 0.1f);
        monitor.evaluateRecord(record);
        verify(sender, times(1)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2003L);
        value.put("batteryLevel", 0.3f);
        monitor.evaluateRecord(record);
        verify(sender, times(2)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2004L);
        value.put("batteryLevel", 0.4f);
        monitor.evaluateRecord(record);
        verify(sender, times(2)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2005L);
        value.put("batteryLevel", 0.01f);
        monitor.evaluateRecord(record);
        verify(sender, times(3)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2005L);
        value.put("batteryLevel", 0.01f);
        monitor.evaluateRecord(record);
        verify(sender, times(3)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2005L);
        value.put("batteryLevel", 0.1f);
        monitor.evaluateRecord(record);
        verify(sender, times(3)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2005L);
        value.put("batteryLevel", 0.1f);
        monitor.evaluateRecord(record);
        verify(sender, times(3)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2005L);
        value.put("batteryLevel", 0.01f);
        monitor.evaluateRecord(record);
        verify(sender, times(4)).sendEmail(anyString(), anyString());

        value.put("timeReceived", 2005L);
        value.put("batteryLevel", 1f);
        monitor.evaluateRecord(record);
        verify(sender, times(5)).sendEmail(anyString(), anyString());
    }
}
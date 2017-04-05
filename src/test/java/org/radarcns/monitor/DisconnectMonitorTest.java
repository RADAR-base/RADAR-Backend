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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.radarcns.util.PersistentStateStore.measurementKeyToString;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import javax.mail.MessagingException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.key.MeasurementKey;
import org.radarcns.monitor.BatteryLevelMonitor.BatteryLevelState;
import org.radarcns.monitor.DisconnectMonitor.DisconnectMonitorState;
import org.radarcns.util.EmailSender;
import org.radarcns.util.PersistentStateStore;

public class DisconnectMonitorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private long offset;
    private long timeReceived;
    private int timesSent;
    private Schema keySchema;
    private Schema valueSchema;
    private EmailSender sender;

    @Before
    public void setUp() {
        Parser parser = new Parser();
        keySchema = parser.parse("{\"name\": \"key\", \"type\": \"record\", \"fields\": ["
                + "{\"name\": \"userId\", \"type\": \"string\"},"
                + "{\"name\": \"sourceId\", \"type\": \"string\"}"
                + "]}");

        valueSchema = parser.parse("{\"name\": \"value\", \"type\": \"record\", \"fields\": ["
                + "{\"name\": \"timeReceived\", \"type\": \"double\"}"
                + "]}");

        offset = 1000L;
        timeReceived = 2000L;
        timesSent = 0;
        sender = mock(EmailSender.class);
    }

    @Test
    public void evaluateRecords() throws Exception {
        ConfigRadar config = KafkaMonitorFactoryTest
                .getBatteryMonitorConfig(25252, folder);
        RadarPropertyHandler properties = KafkaMonitorFactoryTest
                .getRadarPropertyHandler(config, folder);

        long timeout = 50L;

        DisconnectMonitor monitor = new DisconnectMonitor(properties,
                Collections.singletonList("mytopic"), "mygroup", sender, timeout);

        assertEquals(timeout, monitor.getPollTimeout());

        sendMessage(monitor, "1", 0);
        sendMessage(monitor, "2", 0);
        Thread.sleep(timeout + 1);
        monitor.evaluateRecords(new ConsumerRecords<>(Collections.emptyMap()));
        timesSent += 2;
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString());
        sendMessage(monitor, "1", 1);
        sendMessage(monitor, "1", 0);
        sendMessage(monitor, "2", 1);
        sendMessage(monitor, "2", 0);
        Thread.sleep(timeout + 1);
        sendMessage(monitor, "0", 2);
    }

    private void sendMessage(DisconnectMonitor monitor, String source, int sentMessages)
            throws MessagingException {
        Record key = new Record(keySchema);
        key.put("sourceId", source);
        key.put("userId", "me");

        Record value = new Record(valueSchema);
        value.put("timeReceived", timeReceived++);
        ConsumerRecord<GenericRecord, GenericRecord> record = new ConsumerRecord<>(
                "mytopic", 0, offset++, key, value);
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());

        monitor.evaluateRecords(new ConsumerRecords<>(
                Collections.singletonMap(partition, Collections.singletonList(record))));

        timesSent += sentMessages;
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString());
    }

    @Test
    public void retrieveState() throws Exception {
        File base = folder.newFolder();
        PersistentStateStore stateStore = new PersistentStateStore(base);
        DisconnectMonitorState state = new DisconnectMonitorState();
        MeasurementKey key1 = new MeasurementKey("a", "b");
        MeasurementKey key2 = new MeasurementKey("b", "c");
        MeasurementKey key3 = new MeasurementKey("c", "d");
        long now = System.currentTimeMillis();
        state.getLastSeen().put(measurementKeyToString(key1), now);
        state.getLastSeen().put(measurementKeyToString(key2), now + 1L);
        state.getReportedMissing().put(measurementKeyToString(key3), now + 2L);
        stateStore.storeState("one", "two", state);

        PersistentStateStore stateStore2 = new PersistentStateStore(base);
        DisconnectMonitorState state2 = stateStore2.retrieveState("one", "two", new DisconnectMonitorState());
        Map<String, Long> lastSeen = state2.getLastSeen();
        assertThat(lastSeen.size(), is(2));
        assertThat(lastSeen, hasEntry(measurementKeyToString(key1), now));
        assertThat(lastSeen, hasEntry(measurementKeyToString(key2), now + 1L));
        Map<String, Long> reported = state2.getReportedMissing();
        assertThat(reported.size(), is(1));
        assertThat(reported, hasEntry(measurementKeyToString(key3), now + 2L));

    }
}
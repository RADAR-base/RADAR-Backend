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

package org.radarcns.monitor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.radarcns.config.monitor.DisconnectMonitorConfig;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.monitor.DisconnectMonitor.DisconnectMonitorState;
import org.radarcns.monitor.DisconnectMonitor.MissingRecordsReport;
import org.radarcns.util.EmailSender;
import org.radarcns.util.EmailSenders;
import org.radarcns.util.YamlPersistentStateStore;

public class DisconnectMonitorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private long offset;
    private long timeReceived;
    private int timesSent;
    private Schema keySchema;
    private Schema valueSchema;
    private EmailSenders senders;
    private EmailSender sender;

    private static final String PROJECT_ID = "test";

    @Before
    public void setUp() {
        Parser parser = new Parser();
        keySchema = parser.parse("{\"name\": \"key\", \"type\": \"record\", \"fields\": ["
                + "{\"name\": \"projectId\", \"type\": [\"null\", \"string\"]},"
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
        senders = new EmailSenders(Map.of(PROJECT_ID, sender));
    }

    public void evaluateRecords() throws Exception {
        ConfigRadar config = KafkaMonitorFactoryTest
                .getDisconnectMonitorConfig(25252, folder);

        DisconnectMonitorConfig disconnectConfig = config.getDisconnectMonitor();

        disconnectConfig.setTimeout(1L);
        disconnectConfig.setAlertRepeatInterval(2L);
        disconnectConfig.setAlertRepetitions(2);

        Duration timeout = Duration.ofSeconds(disconnectConfig.getTimeout());

        RadarPropertyHandler properties = KafkaMonitorFactoryTest
                .getRadarPropertyHandler(config, folder);

        DisconnectMonitor monitor = new DisconnectMonitor(properties,
                List.of("mytopic"), "mygroup", senders);
        monitor.startScheduler();

        assertEquals(timeout, monitor.getPollTimeout());

        sendMessage(monitor, "1", 0);
        sendMessage(monitor, "1", 1);
        sendMessage(monitor, "2", 0);
        Thread.sleep(timeout.toMillis() + disconnectConfig.getTimeout() * 1000);
        monitor.evaluateRecords(new ConsumerRecords<>(Collections.emptyMap()));
        timesSent += 2;
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString());
        sendMessage(monitor, "1", 1);
        sendMessage(monitor, "1", 0);
        timesSent += 1;
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString());
        sendMessage(monitor, "2", 1);
        sendMessage(monitor, "2", 0);
        sendMessage(monitor, "0", 0);
        timesSent += 1;
        Thread.sleep(timeout.toMillis() + disconnectConfig.getTimeout() * 1000);
        monitor.evaluateRecords(new ConsumerRecords<>(Collections.emptyMap()));
        timesSent += 3;
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString());
    }

    @Test
    public void evaluateRecordsWithScheduledAlerts() throws Exception {
        evaluateRecords();
        Thread.sleep(7_000L);
        timesSent +=6; // executed twice for 3 disconnected devices
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString());
    }

    private void sendMessage(DisconnectMonitor monitor, String source, int sentMessages) {
        Record key = new Record(keySchema);
        key.put("projectId", PROJECT_ID);
        key.put("sourceId", source);
        key.put("userId", "me");

        Record value = new Record(valueSchema);
        value.put("timeReceived", timeReceived++);
        ConsumerRecord<GenericRecord, GenericRecord>record = new ConsumerRecord<>(
            "mytopic", 0, offset++, key, value);
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());

        monitor.evaluateRecords(new ConsumerRecords<>(Map.of(partition, List.of(record))));
    }

    @Test
    public void retrieveState() throws Exception {
        File base = folder.newFolder();
        YamlPersistentStateStore stateStore = new YamlPersistentStateStore(base.toPath());
        DisconnectMonitorState state = new DisconnectMonitorState();
        ObservationKey key1 = new ObservationKey(PROJECT_ID, "a", "b");
        ObservationKey key2 = new ObservationKey(PROJECT_ID, "b", "c");
        ObservationKey key3 = new ObservationKey(PROJECT_ID, "c", "d");
        long now = System.currentTimeMillis();
        state.getLastSeen().put(stateStore.keyToString(key1), now);
        state.getLastSeen().put(stateStore.keyToString(key2), now + 1L);
        state.getReportedMissing().put(stateStore.keyToString(key3), new MissingRecordsReport
                (now -60L, now + 2L, 0));
        stateStore.storeState("one", "two", state);

        YamlPersistentStateStore stateStore2 = new YamlPersistentStateStore(base.toPath());
        DisconnectMonitorState state2 = stateStore2.retrieveState("one", "two", new DisconnectMonitorState());
        Map<String, Long> lastSeen = state2.getLastSeen();
        assertThat(lastSeen.size(), is(2));
        assertThat(lastSeen, hasEntry(stateStore.keyToString(key1), now));
        assertThat(lastSeen, hasEntry(stateStore.keyToString(key2), now + 1L));
        Map<String, MissingRecordsReport> reported = state2.getReportedMissing();
        assertThat(reported.size(), is(1));
        assertThat(reported, hasKey(stateStore.keyToString(key3)));

    }
}
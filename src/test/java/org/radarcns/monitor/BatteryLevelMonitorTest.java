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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.radarcns.monitor.BatteryLevelMonitor.Status.LOW;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import javax.mail.MessagingException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.monitor.BatteryLevelMonitor.BatteryLevelState;
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.util.EmailSender;
import org.radarcns.util.YamlPersistentStateStore;

public class BatteryLevelMonitorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private long offset;
    private long timeReceived;
    private int timesSent;
    private EmailSender sender;

    @Test
    public void evaluateRecord() throws Exception {
        offset = 1000L;
        timeReceived = 2000L;
        timesSent = 0;
        sender = mock(EmailSender.class);

        ConfigRadar config = KafkaMonitorFactoryTest
                .getBatteryMonitorConfig(25252, folder);
        RadarPropertyHandler properties = KafkaMonitorFactoryTest
                .getRadarPropertyHandler(config, folder);

        BatteryLevelMonitor monitor = new BatteryLevelMonitor(properties,
                Collections.singletonList("mytopic"), sender, LOW, 10L);

        sendMessage(monitor, 1.0f, false);
        sendMessage(monitor, 1.0f, false);
        sendMessage(monitor, 0.1f, true);
        sendMessage(monitor, 0.1f, false);
        sendMessage(monitor, 0.3f, false);
        sendMessage(monitor, 0.4f, false);
        sendMessage(monitor, 0.01f, true);
        sendMessage(monitor, 0.01f, false);
        sendMessage(monitor, 0.1f, false);
        sendMessage(monitor, 0.1f, false);
        sendMessage(monitor, 0.01f, true);
        sendMessage(monitor, 1f, false);
    }

    private void sendMessage(BatteryLevelMonitor monitor, float batteryLevel, boolean sentMessage)
            throws MessagingException {
        Record key = new Record(ObservationKey.getClassSchema());
        key.put("projectId", "test");
        key.put("sourceId", "1");
        key.put("userId", "me");

        Record value = new Record(EmpaticaE4BatteryLevel.getClassSchema());
        value.put("time", timeReceived);
        value.put("timeReceived", timeReceived++);
        value.put("batteryLevel", batteryLevel);
        monitor.evaluateRecord(new ConsumerRecord<>("mytopic", 0, offset++, key, value));

        if (sentMessage) {
            timesSent++;
        }
        verify(sender, times(timesSent)).sendEmail(anyString(), anyString());
    }

    @Test
    public void retrieveState() throws Exception {
        File base = folder.newFolder();
        YamlPersistentStateStore stateStore = new YamlPersistentStateStore(base);
        BatteryLevelState state = new BatteryLevelState();
        ObservationKey key1 = new ObservationKey("test", "a", "b");
        String keyString = stateStore.keyToString(key1);
        state.updateLevel(keyString, 0.1f);
        stateStore.storeState("one", "two", state);

        YamlPersistentStateStore stateStore2 = new YamlPersistentStateStore(base);
        BatteryLevelState state2 = stateStore2.retrieveState("one", "two", new BatteryLevelState());
        Map<String, Float> values = state2.getLevels();
        assertThat(values, hasEntry(keyString, 0.1f));
    }
}

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

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SourceStatisticsMonitorConfig;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.stream.aggregator.NumericAggregate;
import org.radarcns.util.PersistentStateStore;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.radarcns.util.PersistentStateStore.measurementKeyToString;

public class SourceStatisticsMonitorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private long offset;
    private long timeReceived;

    @Test
    public void evaluateRecord() throws Exception {
        offset = 1000L;
        timeReceived = 1999L;

        ConfigRadar config = KafkaMonitorFactoryTest
                .getSourceStatisticsMonitorConfig(folder);
        RadarPropertyHandler properties = KafkaMonitorFactoryTest
                .getRadarPropertyHandler(config, folder);

        SourceStatisticsMonitorConfig statConfig = config.getStatisticsMonitors().get(0);
        statConfig.setMaxBatchSize(1);

        SourceStatisticsMonitor actualMonitor = new SourceStatisticsMonitor(properties, statConfig);
        SourceStatisticsMonitor monitor = spy(actualMonitor);
        KafkaSender producer = mock(KafkaSender.class);
        @SuppressWarnings("unchecked")
        KafkaTopicSender<ObservationKey, AggregateKey> sender = mock(KafkaTopicSender.class);
        doReturn(producer).when(monitor).getSender();
        when(producer.<ObservationKey, AggregateKey>sender(any())).thenReturn(sender);
        monitor.setupSender();


        Map<TopicPartition, List<ConsumerRecord<GenericRecord, GenericRecord>>> recordMap;
        recordMap = new HashMap<>();
        recordMap.put(new TopicPartition("android_empatica_e4_battery_level", 0),
                Arrays.asList(getBatteryLevelRecord(), getBatteryLevelRecord()));
        recordMap.put(new TopicPartition("android_empatica_e4_battery_level_10sec", 0),
                Arrays.asList(getAggregateRecord(), getAggregateRecord()));
        ConsumerRecords<GenericRecord, GenericRecord> records = new ConsumerRecords<>(recordMap);
        monitor.evaluateRecords(records);

        verify(sender, times(1)).send(
                new ObservationKey("test", "me", "1"),
                new AggregateKey("test", "me", "1", 1999.0, 2010.0));

        monitor.cleanUpSender();
    }

    private ConsumerRecord<GenericRecord, GenericRecord> getBatteryLevelRecord() {
        Record key = new Record(ObservationKey.getClassSchema());
        key.put("projectId", "test");
        key.put("sourceId", "1");
        key.put("userId", "me");

        Record value = new Record(EmpaticaE4BatteryLevel.getClassSchema());
        value.put("time", (double)timeReceived);
        value.put("timeReceived", (double)timeReceived);
        value.put("batteryLevel", 0.1f);

        timeReceived++;

        return new ConsumerRecord<>("android_empatica_e4_battery_level", 0, offset++, key, value);
    }

    private ConsumerRecord<GenericRecord, GenericRecord> getAggregateRecord() {
        Record key = new Record(AggregateKey.getClassSchema());
        key.put("projectId", "test");
        key.put("sourceId", "1");
        key.put("userId", "me");
        key.put("timeStart", Math.floor(timeReceived / 10) * 10);
        key.put("timeEnd", Math.floor(timeReceived / 10 + 1) * 10);

        timeReceived++;

        Record value = new Record(NumericAggregate.getClassSchema());
        value.put("name", "batteryLevel");
        value.put("min", 0.1);
        value.put("max", 0.1);
        value.put("sum", 0.2);
        value.put("count", 2);
        value.put("mean", 0.1);
        value.put("quartile", Arrays.asList(0.1, 0.1, 0.1));

        return new ConsumerRecord<>("android_empatica_e4_battery_level_10sec", 0, offset++, key, value);
    }

    @Test
    public void retrieveState() throws Exception {
        File base = folder.newFolder();
        PersistentStateStore stateStore = new PersistentStateStore(base);
        SourceStatisticsMonitor.SourceStatistics state = new SourceStatisticsMonitor.SourceStatistics();
        ObservationKey key1 = new ObservationKey("test", "a", "b");
        state.updateSource(key1, 2000.0, 2010.0);
        stateStore.storeState("source_statistics_test", "1", state);

        SourceStatisticsMonitor.SourceStatistics tmpState = new SourceStatisticsMonitor.SourceStatistics();
        assertThat(tmpState.getGroupId(), not(equalTo(state.getGroupId())));

        PersistentStateStore stateStore2 = new PersistentStateStore(base);
        SourceStatisticsMonitor.SourceStatistics state2 = stateStore2.retrieveState("source_statistics_test", "1", tmpState);
        assertThat(state2.getSources(), hasEntry(measurementKeyToString(key1), new AggregateKey("test", "a", "b", 2000.0, 2010.0)));
        assertThat(state2.getUnsent(), hasItem(key1));
        assertThat(state2.getGroupId(), equalTo(state.getGroupId()));

        assertThat(tmpState.getGroupId(), not(equalTo(state.getGroupId())));
    }
}

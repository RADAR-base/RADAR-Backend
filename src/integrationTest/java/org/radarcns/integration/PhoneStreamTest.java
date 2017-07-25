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

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.radarcns.stream.KafkaStreamFactory.PHONE_STREAM;
import static org.radarcns.util.serde.AbstractKafkaAvroSerde.SCHEMA_REGISTRY_CONFIG;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.radarcns.RadarBackend;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.key.MeasurementKey;
import org.radarcns.monitor.AbstractKafkaMonitor;
import org.radarcns.monitor.KafkaMonitor;
import org.radarcns.phone.PhoneUsageEvent;
import org.radarcns.phone.UsageEventType;
import org.radarcns.producer.KafkaTopicSender;
import org.radarcns.producer.direct.DirectSender;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.serde.KafkaAvroSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneStreamTest {
    private static final Logger logger = LoggerFactory.getLogger(PhoneStreamTest.class);
    private static final Map<String, String> CATEGORIES = new HashMap<>();
    static {
        CATEGORIES.put("nl.nos.app", "NEWS_AND_MAGAZINES");
        CATEGORIES.put("nl.thehyve.transmartclient", "MEDICAL");
        CATEGORIES.put("com.twitter.android", "NEWS_AND_MAGAZINES");
        CATEGORIES.put("com.facebook.katana", "SOCIAL");
        CATEGORIES.put("com.nintendo.zara", "GAME_ACTION");
        CATEGORIES.put("com.duolingo", "EDUCATION");
        CATEGORIES.put("com.whatsapp", "COMMUNICATION");
        CATEGORIES.put("com.alibaba.aliexpresshd", "SHOPPING");
        CATEGORIES.put("com.google.android.wearable.app", "COMMUNICATION");
        CATEGORIES.put("com.strava", "HEALTH_AND_FITNESS");
        CATEGORIES.put("com.android.chrome", "COMMUNICATION");
        CATEGORIES.put("com.google.android.youtube", "VIDEO_PLAYERS");
        CATEGORIES.put("com.android.systemui", null);
        CATEGORIES.put("abc.abc", null);
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private RadarPropertyHandler propHandler;
    private RadarBackend backend;

    @Before
    public void setUp() throws IOException, ParseException, InterruptedException {
        String propertiesPath = "src/integrationTest/resources/org/radarcns/kafka/radar.yml";
        propHandler = RadarSingletonFactory.getRadarPropertyHandler();
        if (!propHandler.isLoaded()) {
            propHandler.load(propertiesPath);
        }

        String[] args = {"-c", propertiesPath, "stream"};

        RadarBackendOptions opts = RadarBackendOptions.parse(args);
        propHandler.getRadarProperties().setStreamWorker(PHONE_STREAM);
        backend = new RadarBackend(opts, propHandler);
        backend.start();
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        backend.shutdown();
    }

    @Test(timeout = 300_000L)
    public void testDirect() throws Exception {
        ConfigRadar config = propHandler.getRadarProperties();

        Properties properties = new Properties();
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(SCHEMA_REGISTRY_CONFIG, config.getSchemaRegistry().get(0));
        properties.put(BOOTSTRAP_SERVERS_CONFIG, config.getBrokerPaths());

        DirectSender<MeasurementKey, SpecificRecord> sender = new DirectSender<>(properties);
        AvroTopic<MeasurementKey, PhoneUsageEvent> topic = new AvroTopic<>(
                "android_phone_usage_event",
                MeasurementKey.getClassSchema(), PhoneUsageEvent.getClassSchema(),
                MeasurementKey.class, PhoneUsageEvent.class);

        long offset = 0;
        double time = System.currentTimeMillis() / 1000d - 10d;
        MeasurementKey key = new MeasurementKey("a", "c");
        try (KafkaTopicSender<MeasurementKey, PhoneUsageEvent> topicSender = sender.sender(topic)) {
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time++, "com.whatsapp", null, null, UsageEventType.FOREGROUND));
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time++, "com.whatsapp", null, null, UsageEventType.BACKGROUND));
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time++, "nl.thehyve.transmartclient", null, null, UsageEventType.FOREGROUND));
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time++, "nl.thehyve.transmartclient", null, null, UsageEventType.BACKGROUND));
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time++, "com.strava", null, null, UsageEventType.FOREGROUND));
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time++, "com.strava", null, null, UsageEventType.BACKGROUND));
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time++, "com.android.systemui", null, null, UsageEventType.FOREGROUND));
            topicSender.send(offset++, key, new PhoneUsageEvent(time, time, "com.android.systemui", null, null, UsageEventType.BACKGROUND));
        }
        sender.close();
        consumePhone(offset);
    }

    private void consumePhone(final long numRecordsExpected) throws IOException, InterruptedException {
        String clientId = "someclinet";
        KafkaMonitor monitor = new AbstractKafkaMonitor<GenericRecord, GenericRecord, Object>(RadarSingletonFactory.getRadarPropertyHandler(),
                Collections.singletonList("android_phone_usage_event_output"), "new", clientId, null) {
            int numRecordsRead = 0;
            @Override
            protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> records) {
                logger.info("Read record {} of {}", numRecordsRead, numRecordsExpected);
                GenericRecord value = records.value();
                Double fetchTime = (Double)value.get("categoryNameFetchTime");
                assertNotNull(fetchTime);
                assertTrue(fetchTime > System.currentTimeMillis() / 1000L - 300);
                Object category = value.get("categoryName");
                String packageName = value.get("packageName").toString();
                assertTrue(CATEGORIES.containsKey(packageName));
                String result = CATEGORIES.get(packageName);
                if (result == null) {
                    assertNull(category);
                } else {
                    assertEquals(result, category.toString());
                }

                if (++numRecordsRead == numRecordsExpected) {
                    shutdown();
                }
            }
        };

        monitor.setPollTimeout(280_000L);
        monitor.start();
    }
}

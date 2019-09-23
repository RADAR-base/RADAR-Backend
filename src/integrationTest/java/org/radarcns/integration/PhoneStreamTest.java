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

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.radarbase.config.YamlConfigLoader;
import org.radarbase.mock.MockProducer;
import org.radarbase.mock.config.BasicMockConfig;
import org.radarbase.producer.KafkaTopicSender;
import org.radarbase.producer.direct.DirectSender;
import org.radarbase.topic.AvroTopic;
import org.radarcns.RadarBackend;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.monitor.AbstractKafkaMonitor;
import org.radarcns.monitor.KafkaMonitor;
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;
import org.radarcns.passive.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.passive.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.passive.empatica.EmpaticaE4ElectroDermalActivity;
import org.radarcns.passive.empatica.EmpaticaE4InterBeatInterval;
import org.radarcns.passive.empatica.EmpaticaE4Temperature;
import org.radarcns.passive.phone.PhoneUsageEvent;
import org.radarcns.passive.phone.UsageEventType;
import org.radarcns.schema.registration.KafkaTopics;
import org.radarcns.schema.registration.SchemaRegistry;
import org.radarcns.util.RadarSingleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneStreamTest {
    private static final Logger logger = LoggerFactory.getLogger(PhoneStreamTest.class);
    private static final int MAX_SLEEP = 32;
    private static final AvroTopic<ObservationKey, PhoneUsageEvent> PHONE_USAGE_TOPIC =
            new AvroTopic<>("android_phone_usage_event",
                    ObservationKey.getClassSchema(), PhoneUsageEvent.getClassSchema(),
                    ObservationKey.class, PhoneUsageEvent.class);

    private static final Map<String, String> CATEGORIES = Map.ofEntries(
            Map.entry("nl.nos.app", "NEWS_AND_MAGAZINES"),
            Map.entry("nl.thehyve.transmartclient", "MEDICAL"),
            Map.entry("com.twitter.android", "NEWS_AND_MAGAZINES"),
            Map.entry("com.facebook.katana", "SOCIAL"),
            Map.entry("com.nintendo.zara", "GAME_ACTION"),
            Map.entry("com.duolingo", "EDUCATION"),
            Map.entry("com.whatsapp", "COMMUNICATION"),
            Map.entry("com.alibaba.aliexpresshd", "SHOPPING"),
            Map.entry("com.google.android.wearable.app", "COMMUNICATION"),
            Map.entry("com.strava", "HEALTH_AND_FITNESS"),
            Map.entry("com.android.chrome", "COMMUNICATION"),
            Map.entry("com.google.android.youtube", "VIDEO_PLAYERS"));

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private RadarPropertyHandler propHandler;
    private RadarBackend backend;

    /** Set up tests by sending first data. */
    @Before
    public void setUp() throws IOException, ParseException, InterruptedException {
        String propertiesPath = "src/integrationTest/resources/org/radarcns/kafka/radar.yml";
        propHandler = RadarSingleton.getInstance().getRadarPropertyHandler();
        if (!propHandler.isLoaded()) {
            propHandler.load(propertiesPath);
        }

        ConfigRadar props = propHandler.getRadarProperties();
        KafkaTopics topics = new KafkaTopics(props.getZookeeperPaths());
        int expectedBrokers = props.getBroker().size();
        int activeBrokers = 0;
        int sleep = 2;
        for (int tries = 0; tries < 10; tries++) {
            activeBrokers = topics.getNumberOfBrokers();
            if (activeBrokers >= expectedBrokers) {
                logger.info("Kafka brokers available. Starting topic creation.");
                break;
            } else {
                logger.warn("Only {} out of {} Kafka brokers available. Waiting {} seconds.",
                        activeBrokers, expectedBrokers, sleep);
                Thread.sleep(sleep * 1000L);
                sleep = Math.min(MAX_SLEEP, sleep * 2);
            }
        }
        assertThat(activeBrokers, greaterThanOrEqualTo(expectedBrokers));

        topics.createTopics(Stream.of(
                "android_phone_usage_event", "android_phone_usage_event_output",
                "android_phone_usage_event_aggregated", "android_empatica_e4_acceleration",
                "android_empatica_e4_acceleration_10sec"),
                3, (short)1);

        SchemaRegistry registry = new SchemaRegistry(props.getSchemaRegistryPaths());
        registry.registerSchema(PHONE_USAGE_TOPIC);
        registry.registerSchema(new AvroTopic<>("android_empatica_e4_acceleration",
                ObservationKey.getClassSchema(), EmpaticaE4Acceleration.getClassSchema(),
                ObservationKey.class, EmpaticaE4Acceleration.class));
        registry.registerSchema(new AvroTopic<>("android_empatica_e4_battery_level",
                ObservationKey.getClassSchema(), EmpaticaE4BatteryLevel.getClassSchema(),
                ObservationKey.class, EmpaticaE4BatteryLevel.class));
        registry.registerSchema(new AvroTopic<>("android_empatica_e4_blood_volume_pulse",
                ObservationKey.getClassSchema(), EmpaticaE4BloodVolumePulse.getClassSchema(),
                ObservationKey.class, EmpaticaE4BloodVolumePulse.class));
        registry.registerSchema(new AvroTopic<>("android_empatica_e4_electrodermal_activity",
                ObservationKey.getClassSchema(), EmpaticaE4ElectroDermalActivity.getClassSchema(),
                ObservationKey.class, EmpaticaE4ElectroDermalActivity.class));
        registry.registerSchema(new AvroTopic<>("android_empatica_e4_inter_beat_interval",
                ObservationKey.getClassSchema(), EmpaticaE4InterBeatInterval.getClassSchema(),
                ObservationKey.class, EmpaticaE4InterBeatInterval.class));
        registry.registerSchema(new AvroTopic<>("android_empatica_e4_temperature",
                ObservationKey.getClassSchema(), EmpaticaE4Temperature.getClassSchema(),
                ObservationKey.class, EmpaticaE4Temperature.class));

        String[] args = {"-c", propertiesPath, "stream"};
        RadarBackendOptions opts = RadarBackendOptions.parse(args);
        backend = new RadarBackend(opts, propHandler);
        backend.start();
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        backend.shutdown();
    }

    @Test(timeout = 600_000L)
    public void testDirect() throws Exception {
        ConfigRadar config = propHandler.getRadarProperties();

        Properties properties = new Properties();
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryPaths());
        properties.put(BOOTSTRAP_SERVERS_CONFIG, config.getBrokerPaths());

        DirectSender sender = new DirectSender(properties);

        double time = System.currentTimeMillis() / 1000d - 10d;
        ObservationKey key = new ObservationKey("test", "a", "c");

        List<PhoneUsageEvent> events = List.of(
                new PhoneUsageEvent(time, time++,
                        "com.whatsapp", null, null, UsageEventType.FOREGROUND),
                new PhoneUsageEvent(time, time++,
                        "com.whatsapp", null, null, UsageEventType.BACKGROUND),
                new PhoneUsageEvent(time, time++,
                        "nl.thehyve.transmartclient", null, null, UsageEventType.FOREGROUND),
                new PhoneUsageEvent(time, time++,
                        "nl.thehyve.transmartclient", null, null, UsageEventType.BACKGROUND),
                new PhoneUsageEvent(time, time++,
                        "com.strava", null, null, UsageEventType.FOREGROUND),
                new PhoneUsageEvent(time, time++,
                        "com.strava", null, null, UsageEventType.BACKGROUND),
                new PhoneUsageEvent(time, time++,
                        "com.android.systemui", null, null, UsageEventType.FOREGROUND),
                new PhoneUsageEvent(time, time,
                        "com.android.systemui", null, null, UsageEventType.BACKGROUND));

        try (KafkaTopicSender<ObservationKey, PhoneUsageEvent> topicSender =
                sender.sender(PHONE_USAGE_TOPIC)) {
            for (PhoneUsageEvent event : events) {
                topicSender.send(key, event);
            }
        }

        sender.close();

        Path file = Paths.get(getClass().getResource("/mock_devices.yml").getFile());
        BasicMockConfig mockConfig = new YamlConfigLoader().load(file, BasicMockConfig.class);

        MockProducer mockProducer = new MockProducer(mockConfig);
        mockProducer.start();
        Thread.sleep(mockConfig.getDuration());
        mockProducer.shutdown();

        consumePhone();
        consumeAggregated();
        consumeE4();
    }

    private void consumeE4() {
        String clientId = "consumeE4";
        E4AggregatedAccelerationMonitor monitor = new E4AggregatedAccelerationMonitor(
                RadarSingleton.getInstance().getRadarPropertyHandler(),
                "android_empatica_e4_acceleration_10sec", clientId);
        monitor.setPollTimeout(Duration.ofMinutes(5).minus(Duration.ofSeconds(20)));
        monitor.start();
    }

    private void consumePhone() throws IOException, InterruptedException {
        String clientId = "consumePhone";
        KafkaMonitor monitor = new PhoneOutputMonitor(
                RadarSingleton.getInstance().getRadarPropertyHandler(), clientId,8L);

        monitor.setPollTimeout(Duration.ofMinutes(5).minusSeconds(20));
        monitor.start();
    }

    private void consumeAggregated() throws IOException, InterruptedException {
        String clientId = "consumeAggregated";
        KafkaMonitor monitor = new PhoneAggregateMonitor(
                RadarSingleton.getInstance().getRadarPropertyHandler(), clientId, 4L);

        monitor.setPollTimeout(Duration.ofMinutes(5).minusSeconds(20));
        monitor.start();
    }


    private static class PhoneOutputMonitor
            extends AbstractKafkaMonitor<GenericRecord, GenericRecord, Object> {
        private final long numRecordsExpected;
        int numRecordsRead;

        public PhoneOutputMonitor(RadarPropertyHandler radar, String clientId,
                long numRecordsExpected) {
            super(radar, List.of("android_phone_usage_event_output"), clientId, clientId, null);
            this.numRecordsExpected = numRecordsExpected;

            Properties props = new Properties();
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.putAll(radar.getRadarProperties().getStream().getProperties());
            configure(props);
            numRecordsRead = 0;
        }

        @Override
        protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
            logger.info("Read phone usage output {} of {} with value {}", ++numRecordsRead,
                    numRecordsExpected, record.value());
            GenericRecord value = record.value();
            Double fetchTime = (Double)value.get("categoryNameFetchTime");
            assertNotNull(fetchTime);
            assertThat(fetchTime, greaterThan(System.currentTimeMillis() / 1000L - 300d));
            Object category = value.get("categoryName");
            String packageName = value.get("packageName").toString();
            assertThat(packageName, isIn(CATEGORIES.keySet()));
            String result = CATEGORIES.get(packageName);
            if (result == null) {
                assertNull(category);
            } else {
                assertEquals(result, category.toString());
            }

            if (numRecordsRead == numRecordsExpected) {
                shutdown();
            }
        }
    }

    private static class PhoneAggregateMonitor
            extends AbstractKafkaMonitor<GenericRecord, GenericRecord, Object> {
        private final long numRecordsExpected;
        int numRecordsRead;

        public PhoneAggregateMonitor(RadarPropertyHandler radar, String clientId,
                long numRecordsExpected) {
            super(radar, List.of("android_phone_usage_event_aggregated"), clientId, clientId, null);
            this.numRecordsExpected = numRecordsExpected;
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.putAll(radar.getRadarProperties().getStream().getProperties());
            configure(props);
            numRecordsRead = 0;
        }

        @Override
        protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
            logger.info("Read phone aggregate output {} of {} with value {}", ++numRecordsRead,
                    numRecordsExpected, record.value());
            GenericRecord value = record.value();
            int timesOpen = (int)value.get("timesOpen");
            assertThat(timesOpen, greaterThan(0));
            if (numRecordsRead == numRecordsExpected) {
                shutdown();
            }
        }
    }
}

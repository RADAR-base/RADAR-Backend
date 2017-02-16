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

package org.radarcns.integration;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.radarcns.RadarBackend;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.key.MeasurementKey;
import org.radarcns.mock.MockDevice;
import org.radarcns.producer.KafkaSender;
import org.radarcns.producer.direct.DirectSender;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectProducerTest {

    private final static Logger logger = LoggerFactory.getLogger(DirectProducerTest.class);
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testDirect() throws Exception {
        String propertiesPath = "src/integrationTest/resources/org/radarcns/kafka/radar.yml";

        RadarPropertyHandler properties = RadarSingletonFactory.getRadarPropertyHandler();
        properties.load(propertiesPath);

        ConfigRadar config = properties.getRadarProperties();

        Properties props = new Properties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryPaths());
        props.put(BOOTSTRAP_SERVERS_CONFIG, config.getBrokerPaths());
        props.put(KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);

        int numberOfDevices = 1;
        logger.info("Simulating the load of " + numberOfDevices);

        String userID = "UserID_";
        String sourceID = "SourceID_";

        MockDevice[] threads = new MockDevice[numberOfDevices];
        KafkaSender[] senders = new KafkaSender[numberOfDevices];
        for (int i = 0; i < numberOfDevices; i++) {
            senders[i] = new DirectSender(props);
            //noinspection unchecked
            threads[i] = new MockDevice<>(senders[i], new MeasurementKey(userID+i, sourceID+i), MeasurementKey.getClassSchema(), MeasurementKey.class);
            threads[i].start();
        }
        long streamingTimeoutMs = 1_000L;

        Map<String, Object> extras = config.getExtras();
        if (extras != null && extras.containsKey("streaming_timeout_ms")) {
            streamingTimeoutMs = ((Number)extras.get("streaming_timeout_ms")).longValue();
        }

        threads[0].join(streamingTimeoutMs);
        for (MockDevice device : threads) {
            device.interrupt();
        }
        for (MockDevice device : threads) {
            device.join();
        }
        for (KafkaSender sender : senders) {
            try {
                sender.close();
            } catch (IOException e) {
                logger.warn("Failed to close sender", e);
            }
        }
        for (MockDevice device : threads) {
            assertNull("Device had IOException", device.getException());
        }

        String[] args = {"-c", propertiesPath};

        RadarBackendOptions opts = RadarBackendOptions.parse(args);
        new RadarBackend(opts, properties).application();

        Thread.sleep(40_000L);

        consumeAggregated();
    }

    private void consumeAggregated() throws IOException {
        String clientId = "someclinet";
        E4AggregatedAccelerationMonitor monitor = new E4AggregatedAccelerationMonitor(
                RadarSingletonFactory.getRadarPropertyHandler(),
                "android_empatica_e4_acceleration_output", clientId);
        monitor.setPollTimeout(100_000L);
        monitor.start();
    }
}

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

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.radarcns.RadarBackend;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.kafka.KafkaSender;
import org.radarcns.key.MeasurementKey;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectProducerTest {

    private final static Logger logger = LoggerFactory.getLogger(DirectProducerTest.class);
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testDirect() throws Exception {

        Properties props = new Properties();
        try (InputStream in = getClass().getResourceAsStream("/org/radarcns/kafka/kafka.properties")) {
            assertNotNull(in);
            props.load(in);
            in.close();
        }

        if (!Boolean.parseBoolean(props.getProperty("servertest","false"))) {
            logger.info("Serve test case has been disable.");
            return;
        }

        int numberOfDevices = 1;
        logger.info("Simulating the load of " + numberOfDevices);

        String userID = "UserID_";
        String sourceID = "SourceID_";

        MockDevice[] threads = new MockDevice[numberOfDevices];
        KafkaSender[] senders = new KafkaSender[numberOfDevices];
        for (int i = 0; i < numberOfDevices; i++) {
            senders[i] = new DirectProducer<>(props);
            //noinspection unchecked
            threads[i] = new MockDevice<>(senders[i], new MeasurementKey(userID+i, sourceID+i), MeasurementKey.getClassSchema(), MeasurementKey.class);
            threads[i].start();
        }
        long streamingTimeoutMs = 5_000L;
        if (props.containsKey("streaming.timeout.ms")) {
            try {
                streamingTimeoutMs = Long.parseLong(props.getProperty("streaming.timeout.ms"));
            } catch (NumberFormatException ex) {
                // do nothing
            }
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

        String[] args = {"-c", "src/integrationTest/resources/org/radarcns/kafka/radar.yml"};

        RadarBackendOptions opts = RadarBackendOptions.parse(args);
        new RadarBackend(opts).application();

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

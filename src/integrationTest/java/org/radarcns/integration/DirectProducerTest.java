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

import static org.radarcns.stream.KafkaStreamFactory.ALL_STREAMS;

import java.io.File;
import java.io.IOException;
import org.apache.commons.cli.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.radarcns.RadarBackend;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.mock.MockProducer;
import org.radarcns.mock.config.BasicMockConfig;
import org.radarcns.util.RadarSingletonFactory;

public class DirectProducerTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private RadarBackend backend;

    @Before
    public void setUp() throws IOException, ParseException, InterruptedException {
        String propertiesPath = "src/integrationTest/resources/org/radarcns/kafka/radar.yml";
        RadarPropertyHandler propHandler = RadarSingletonFactory.getRadarPropertyHandler();
        if (!propHandler.isLoaded()) {
            propHandler.load(propertiesPath);
        }

        String[] args = {"-c", propertiesPath, "stream"};

        RadarBackendOptions opts = RadarBackendOptions.parse(args);
        propHandler.getRadarProperties().setStreamWorker(ALL_STREAMS);
        backend = new RadarBackend(opts, propHandler);
        backend.start();
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        backend.shutdown();
    }

    @Test(timeout = 300_000L)
    public void testDirect() throws Exception {
        File file = new File(getClass().getResource("/mock_devices.yml").getFile());
        BasicMockConfig mockConfig = new YamlConfigLoader().load(file, BasicMockConfig.class);

        MockProducer mockProducer = new MockProducer(mockConfig);
        mockProducer.start();
        Thread.sleep(mockConfig.getDuration());
        mockProducer.shutdown();

        String clientId = "someclinet";
        E4AggregatedAccelerationMonitor monitor = new E4AggregatedAccelerationMonitor(
                RadarSingletonFactory.getRadarPropertyHandler(),
                "android_empatica_e4_acceleration_output", clientId);
        monitor.setPollTimeout(280_000L);
        monitor.start();
    }
}

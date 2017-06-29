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

import java.io.File;
import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectProducerTest {

    private final static Logger logger = LoggerFactory.getLogger(DirectProducerTest.class);
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testDirect() throws Exception {
        File file = new File(getClass().getResource("/mock_devices.yml").getFile());
        BasicMockConfig mockConfig = new YamlConfigLoader().load(file, BasicMockConfig.class);

        MockProducer mockProducer = new MockProducer(mockConfig);
        mockProducer.start();
        Thread.sleep(mockConfig.getDuration());
        mockProducer.shutdown();

        String propertiesPath = "src/integrationTest/resources/org/radarcns/kafka/radar.yml";
        RadarPropertyHandler properties = RadarSingletonFactory.getRadarPropertyHandler();
        properties.load(propertiesPath);

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

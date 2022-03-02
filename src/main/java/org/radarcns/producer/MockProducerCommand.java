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

package org.radarcns.producer;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.SchemaValidationException;
import org.radarbase.mock.MockProducer;
import org.radarbase.mock.config.BasicMockConfig;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.MockConfig;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockProducerCommand implements SubCommand {
    private static final Logger logger = LoggerFactory.getLogger(MockProducerCommand.class);
    private final MockProducer producer;

    public MockProducerCommand(RadarBackendOptions options,
            RadarPropertyHandler radarPropertyHandler) throws IOException {
        ConfigRadar radar = radarPropertyHandler.getRadarProperties();

        BasicMockConfig producerConfig = new BasicMockConfig();

        Path mockFile = options.getMockFile();

        if (mockFile != null) {
            MockConfig mockConfig = radarPropertyHandler.getLoader().load(mockFile, MockConfig.class);
            producerConfig.setData(mockConfig.getData());
        } else {
            producerConfig.setNumberOfDevices(options.getNumMockDevices());
        }
        producerConfig.setAuthConfig(radar.getAuth());
        producerConfig.setRestProxy(radar.getRestProxy());
        producerConfig.setSchemaRegistry(radar.getSchemaRegistry().get(0));
        producerConfig.setProducerMode(options.isMockDirect() ? "direct" : "rest");
        producer = new MockProducer(producerConfig);
    }

    @Override
    public void start() throws IOException {
        producer.start();
    }

    @Override
    public void shutdown() throws IOException, InterruptedException {
        try {
            producer.shutdown();
        } catch (SchemaValidationException e) {
            logger.error("Data did not match schema", e);
        }
    }
}

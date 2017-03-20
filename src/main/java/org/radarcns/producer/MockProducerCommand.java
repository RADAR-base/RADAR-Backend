package org.radarcns.producer;

import java.io.File;
import java.io.IOException;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.MockConfig;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SubCommand;
import org.radarcns.mock.BasicMockConfig;
import org.radarcns.mock.MockProducer;

public class MockProducerCommand implements SubCommand {
    private final MockProducer producer;

    public MockProducerCommand(RadarBackendOptions options,
            RadarPropertyHandler radarPropertyHandler) throws IOException {
        ConfigRadar radar = radarPropertyHandler.getRadarProperties();

        BasicMockConfig producerConfig = new BasicMockConfig();

        File mockFile = options.getMockFile();

        if (mockFile != null) {
            MockConfig mockConfig = new YamlConfigLoader().load(mockFile, MockConfig.class);
            producerConfig.setData(mockConfig.getData());
        } else {
            producerConfig.setNumberOfDevices(options.getNumMockDevices());
        }
        producerConfig.setRestProxy(radar.getRestProxy());
        producerConfig.setSchemaRegistry(radar.getSchemaRegistry().get(0));
        producerConfig.setProducerMode(options.isMockDirect() ? "direct" : "rest");
        producer = new MockProducer(producerConfig);
    }

    @Override
    public void start() throws IOException, InterruptedException {
        producer.start();
    }

    @Override
    public void shutdown() throws IOException, InterruptedException {
        producer.shutdown();
    }
}

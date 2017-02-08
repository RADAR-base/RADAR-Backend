package org.radarcns.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SubCommand;
import org.radarcns.key.MeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockProducerCommand implements SubCommand {
    private static final Logger logger = LoggerFactory.getLogger(MockProducerCommand.class);

    private final List<MockDevice> devices;
    private final List<KafkaSender<MeasurementKey, SpecificRecord>> senders;


    public MockProducerCommand(RadarBackendOptions options,
            RadarPropertyHandler radarPropertyHandler) {
        int numDevices = options.getNumMockDevices();

        logger.info("Simulating the load of " + numDevices);

        String userId = "UserID_";
        String sourceId = "SourceID_";

        devices = new ArrayList<>(numDevices);
        senders = new ArrayList<>(numDevices);

        ConfigRadar radar = radarPropertyHandler.getRadarProperties();
        SchemaRetriever retriever = new SchemaRetriever(radar.getSchemaRegistryPaths());
        RestSender<MeasurementKey, SpecificRecord> firstSender = new RestSender<>(
                radar.getRestProxyPath(), retriever,
                new SpecificRecordEncoder(false), new SpecificRecordEncoder(false),
                10_000);

        for (int i = 0; i < numDevices; i++) {
            senders.add(new BatchedKafkaSender<>(firstSender, 10_000, 1000));
            //noinspection unchecked
            devices.add(new MockDevice<>(senders.get(i), new MeasurementKey(userId + i,
                    sourceId + i), MeasurementKey.getClassSchema(), MeasurementKey.class));
        }
    }

    @Override
    public void start() throws IOException, InterruptedException {
        for (MockDevice device : devices) {
            device.start();
        }
    }

    @Override
    public void shutdown() throws IOException, InterruptedException {
        logger.info("Shutting down mock devices");
        for (MockDevice device : devices) {
            device.shutdown();
        }
        logger.info("Waiting for mock devices to finish...");
        for (MockDevice device : devices) {
            device.join(5_000L);
        }
        logger.info("Closing channels");
        for (KafkaSender<MeasurementKey, SpecificRecord> sender : senders) {
            sender.close();
        }
    }
}

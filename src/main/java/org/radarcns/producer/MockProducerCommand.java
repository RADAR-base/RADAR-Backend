package org.radarcns.producer;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.config.ConfigLoader;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.MockConfig;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SubCommand;
import org.radarcns.data.SpecificRecordEncoder;
import org.radarcns.key.MeasurementKey;
import org.radarcns.mock.MockDevice;
import org.radarcns.mock.MockFile;
import org.radarcns.producer.direct.DirectSender;
import org.radarcns.producer.rest.BatchedKafkaSender;
import org.radarcns.producer.rest.RestSender;
import org.radarcns.producer.rest.SchemaRetriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockProducerCommand implements SubCommand {
    private static final Logger logger = LoggerFactory.getLogger(MockProducerCommand.class);

    private final List<MockDevice<MeasurementKey>> devices;
    private final List<MockFile> files;
    private final List<KafkaSender<MeasurementKey, SpecificRecord>> senders;

    public MockProducerCommand(RadarBackendOptions options,
            RadarPropertyHandler radarPropertyHandler) throws IOException {
        ConfigRadar radar = radarPropertyHandler.getRadarProperties();

        File mockFile = options.getMockFile();
        MockConfig mockConfig = null;

        int numDevices;
        if (mockFile != null) {
            mockConfig = new ConfigLoader().load(mockFile, MockConfig.class);
            numDevices = mockConfig.getData().size();
        } else {
            numDevices = options.getNumMockDevices();
        }

        logger.info("Simulating the load of " + numDevices);

        String userId = "UserID_";
        String sourceId = "SourceID_";

        devices = new ArrayList<>(numDevices);
        senders = new ArrayList<>(numDevices);
        files = new ArrayList<>(numDevices);

        if (options.isMockDirect()) {
            for (int i = 0; i < numDevices; i++) {
                Properties properties = new Properties();
                properties.put(KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                properties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                ConfigRadar radarProperties = radarPropertyHandler.getRadarProperties();
                properties.put(SCHEMA_REGISTRY_URL_CONFIG, radarProperties.getSchemaRegistryPaths());
                properties.put(BOOTSTRAP_SERVERS_CONFIG, radarProperties.getBrokerPaths());

                senders.add(new DirectSender(properties));
            }
        } else {
            SchemaRetriever retriever = new SchemaRetriever(radar.getSchemaRegistryPaths());

            RestSender<MeasurementKey, SpecificRecord> firstSender = new RestSender<>(new URL(
                    radar.getRestProxyPath()), retriever,
                    new SpecificRecordEncoder(false), new SpecificRecordEncoder(false),
                    10_000);
            for (int i = 0; i < numDevices; i++) {
                senders.add(new BatchedKafkaSender<>(firstSender, 10_000, 1000));
            }
        }

        if (mockConfig == null) {
            for (int i = 0; i < numDevices; i++) {
                devices.add(new MockDevice<>(senders.get(i), new MeasurementKey(userId + i,
                        sourceId + i), MeasurementKey.getClassSchema(), MeasurementKey.class));
            }
        } else {
            try {
                for (int i = 0; i < numDevices; i++) {
                    files.add(new MockFile(senders.get(i), mockFile, mockConfig.getData().get(i)));
                }
            } catch (NoSuchMethodException | IllegalAccessException
                    | InvocationTargetException | ClassNotFoundException ex) {
                throw new IOException("Cannot instantiate mock file", ex);
            }
        }
    }

    @Override
    public void start() throws IOException, InterruptedException {
        for (MockDevice device : devices) {
            device.start();
        }
        for (MockFile file : files) {
            file.send();
        }
    }

    @Override
    public void shutdown() throws IOException, InterruptedException {
        if (devices.isEmpty()) {
            return;
        }
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

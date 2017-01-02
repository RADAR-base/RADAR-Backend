package org.radarcns.integration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.radarcns.RadarBackend;
import org.radarcns.kafka.KafkaSender;
import org.radarcns.key.MeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertNull;

public class DirectProducerTest {

    private final static Logger logger = LoggerFactory.getLogger(DirectProducerTest.class);
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private String clientID = "someclinet";
    protected static int numOfAggregatedRecords = 5;

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

        String[] args = {"src/integrationTest/resources/org/radarcns/kafka/radar.yml"};

        RadarBackend backend = new RadarBackend(args);

        Thread.sleep(numOfAggregatedRecords*11_000L);

        consumeAggregated();
    }

    private void consumeAggregated() {
        E4AggregatedAccelerationMonitor monitor = new E4AggregatedAccelerationMonitor("android_empatica_e4_acceleration_output", clientID);
        monitor.monitor(100_000L);
    }


}

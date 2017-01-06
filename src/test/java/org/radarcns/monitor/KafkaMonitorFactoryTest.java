package org.radarcns.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.radarcns.config.BatteryMonitorConfig;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandlerImpl;

public class KafkaMonitorFactoryTest {
    @Test
    public void createMonitor() throws Exception {
        String[] args = {"monitor", "battery"};
        RadarBackendOptions options = RadarBackendOptions.parse(args);
        ConfigRadar config = new ConfigRadar();
        BatteryMonitorConfig batteryConfig = new BatteryMonitorConfig();
        batteryConfig.setEmailAddress("test@localhost");
        batteryConfig.setLevel("LOW");
        config.setBatteryMonitor(batteryConfig);
        String tmpDir = Files.createTempDirectory(null).toAbsolutePath().toString();
        config.setPersistencePath(tmpDir);
        config.setSchemaRegistry(Collections.emptyList());
        config.setBroker(Collections.emptyList());

        File tmpConfig = File.createTempFile("radar", ".yml");
        config.store(tmpConfig);

        RadarPropertyHandler properties = new RadarPropertyHandlerImpl();
        properties.load(tmpConfig.getAbsolutePath());

        KafkaMonitor monitor = new KafkaMonitorFactory(options, properties).createMonitor();
        assertEquals(BatteryLevelMonitor.class, monitor.getClass());
        BatteryLevelMonitor batteryMonitor = (BatteryLevelMonitor) monitor;
        batteryMonitor.evaluateRecords(new ConsumerRecords<>(Collections.emptyMap()));
        assertTrue(new File(tmpDir, "battery_monitors_1.yml").isFile());
    }
}
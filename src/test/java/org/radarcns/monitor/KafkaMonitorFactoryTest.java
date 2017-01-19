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

package org.radarcns.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.radarcns.config.BatteryMonitorConfig;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.DisconnectMonitorConfig;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandlerImpl;
import org.radarcns.util.EmailServerRule;

public class KafkaMonitorFactoryTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @ClassRule
    public static EmailServerRule emailServer = new EmailServerRule(25251);

    @Test
    public void createBatteryMonitor() throws Exception {
        String[] args = {"monitor", "battery"};
        RadarBackendOptions options = RadarBackendOptions.parse(args);
        ConfigRadar config = getBatteryMonitorConfig(emailServer.getPort(), folder);
        RadarPropertyHandler properties = getRadarPropertyHandler(config, folder);

        KafkaMonitor monitor = new KafkaMonitorFactory(options, properties).createMonitor();
        assertEquals(BatteryLevelMonitor.class, monitor.getClass());
        BatteryLevelMonitor batteryMonitor = (BatteryLevelMonitor) monitor;
        batteryMonitor.evaluateRecords(new ConsumerRecords<>(Collections.emptyMap()));
        assertTrue(new File(config.getPersistencePath(), "battery_monitors_1.yml").isFile());
    }

    @Test(expected = IOException.class)
    public void createBatteryMonitorWithoutEmailServer() throws Exception {
        String[] args = {"monitor", "battery"};
        RadarBackendOptions options = RadarBackendOptions.parse(args);
        ConfigRadar config = getBatteryMonitorConfig(emailServer.getPort() + 1, folder);
        RadarPropertyHandler properties = getRadarPropertyHandler(config, folder);

        KafkaMonitor monitor = new KafkaMonitorFactory(options, properties).createMonitor();
    }

    @Test
    public void createDisconnectMonitor() throws Exception {
        String[] args = {"monitor", "disconnect"};
        RadarBackendOptions options = RadarBackendOptions.parse(args);
        ConfigRadar config = getDisconnectMonitorConfig(emailServer.getPort(), folder);
        RadarPropertyHandler properties = getRadarPropertyHandler(config, folder);

        KafkaMonitor monitor = new KafkaMonitorFactory(options, properties).createMonitor();
        assertEquals(DisconnectMonitor.class, monitor.getClass());
        DisconnectMonitor disconnectMonitor = (DisconnectMonitor) monitor;
        disconnectMonitor.evaluateRecords(new ConsumerRecords<>(Collections.emptyMap()));
        assertTrue(new File(config.getPersistencePath(), "temperature_disconnect_1.yml").isFile());
    }

    @Test
    public void createAllMonitor() throws Exception {
        String[] args = {"monitor", "all"};
        RadarBackendOptions options = RadarBackendOptions.parse(args);
        ConfigRadar config = createBasicConfig(folder);
        config.setBatteryMonitor(getBatteryMonitorConfig(emailServer.getPort()));
        config.setDisconnectMonitor(getDisconnectMonitorConfig(emailServer.getPort()));
        RadarPropertyHandler properties = getRadarPropertyHandler(config, folder);

        KafkaMonitor monitor = new KafkaMonitorFactory(options, properties).createMonitor();
        assertEquals(CombinedKafkaMonitor.class, monitor.getClass());
        CombinedKafkaMonitor combinedMonitor = (CombinedKafkaMonitor) monitor;
        List<KafkaMonitor> monitors = ((CombinedKafkaMonitor) monitor).getMonitors();
        assertEquals(2, monitors.size());
        assertEquals(BatteryLevelMonitor.class, monitors.get(0).getClass());
        assertEquals(DisconnectMonitor.class, monitors.get(1).getClass());
    }

    public static RadarPropertyHandler getRadarPropertyHandler(ConfigRadar config, TemporaryFolder folder) throws IOException {
        File tmpConfig = folder.newFile("radar.yml");
        config.store(tmpConfig);

        RadarPropertyHandler properties = new RadarPropertyHandlerImpl();
        properties.load(tmpConfig.getAbsolutePath());
        return properties;
    }

    public static ConfigRadar createBasicConfig(TemporaryFolder folder) throws IOException {
        ConfigRadar config = new ConfigRadar();
        config.setPersistencePath(folder.newFolder().getAbsolutePath());
        config.setSchemaRegistry(Collections.emptyList());
        config.setBroker(Collections.emptyList());
        return config;
    }

    public static DisconnectMonitorConfig getDisconnectMonitorConfig(int port) {
        DisconnectMonitorConfig disconnectConfig = new DisconnectMonitorConfig();
        disconnectConfig.setEmailAddress("test@localhost");
        disconnectConfig.setEmailHost("localhost");
        disconnectConfig.setEmailPort(port);
        disconnectConfig.setTimeout(100L);
        return disconnectConfig;
    }

    public static ConfigRadar getDisconnectMonitorConfig(int port, TemporaryFolder folder) throws IOException {
        ConfigRadar config = createBasicConfig(folder);
        config.setDisconnectMonitor(getDisconnectMonitorConfig(port));
        return config;
    }

    public static BatteryMonitorConfig getBatteryMonitorConfig(int port) {
        BatteryMonitorConfig batteryConfig = new BatteryMonitorConfig();
        batteryConfig.setEmailAddress("test@localhost");
        batteryConfig.setEmailHost("localhost");
        batteryConfig.setEmailPort(port);
        batteryConfig.setLevel("LOW");
        batteryConfig.setEmailUser("someuser");
        return batteryConfig;
    }

    public static ConfigRadar getBatteryMonitorConfig(int port, TemporaryFolder folder) throws IOException {
        ConfigRadar config = createBasicConfig(folder);
        config.setBatteryMonitor(getBatteryMonitorConfig(port));
        return config;
    }
}
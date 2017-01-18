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
import java.nio.file.Files;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.Before;
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
import org.subethamail.wiser.Wiser;

public class KafkaMonitorFactoryTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public EmailServerRule emailServer = new EmailServerRule(25251);

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

    public static RadarPropertyHandler getRadarPropertyHandler(ConfigRadar config, TemporaryFolder folder) throws IOException {
        File tmpConfig = folder.newFile("radar.yml");
        config.store(tmpConfig);

        RadarPropertyHandler properties = new RadarPropertyHandlerImpl();
        properties.load(tmpConfig.getAbsolutePath());
        return properties;
    }

    public static ConfigRadar getDisconnectMonitorConfig(int port, TemporaryFolder folder) throws IOException {
        ConfigRadar config = new ConfigRadar();
        DisconnectMonitorConfig disconnectConfig = new DisconnectMonitorConfig();
        disconnectConfig.setEmailAddress("test@localhost");
        disconnectConfig.setEmailHost("localhost");
        disconnectConfig.setEmailPort(port);
        disconnectConfig.setTimeout(100L);
        config.setPersistencePath(folder.newFolder().getAbsolutePath());
        config.setSchemaRegistry(Collections.emptyList());
        config.setBroker(Collections.emptyList());
        return config;
    }

    public static ConfigRadar getBatteryMonitorConfig(int port, TemporaryFolder folder) throws IOException {
        ConfigRadar config = new ConfigRadar();
        BatteryMonitorConfig batteryConfig = new BatteryMonitorConfig();
        batteryConfig.setEmailAddress("test@localhost");
        batteryConfig.setEmailHost("localhost");
        batteryConfig.setEmailPort(port);
        batteryConfig.setLevel("LOW");
        batteryConfig.setEmailUser("someuser");
        config.setBatteryMonitor(batteryConfig);
        config.setPersistencePath(folder.newFolder().getAbsolutePath());
        config.setSchemaRegistry(Collections.emptyList());
        config.setBroker(Collections.emptyList());
        return config;
    }
}
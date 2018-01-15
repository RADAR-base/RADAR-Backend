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

package org.radarcns.monitor;

import org.radarcns.config.BatteryMonitorConfig;
import org.radarcns.config.DisconnectMonitorConfig;
import org.radarcns.config.MonitorConfig;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SourceStatisticsMonitorConfig;
import org.radarcns.util.EmailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class KafkaMonitorFactory {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMonitorFactory.class);

    private final RadarPropertyHandler properties;
    private final RadarBackendOptions options;

    public KafkaMonitorFactory(RadarBackendOptions options,
            RadarPropertyHandler properties) {
        this.options = options;
        this.properties = properties;
    }

    public KafkaMonitor createMonitor() throws IOException {
        KafkaMonitor monitor;
        String[] args = options.getSubCommandArgs();
        String commandType;
        if (args == null || args.length == 0) {
            commandType = "all";
        } else {
            commandType = args[0];
        }

        switch (commandType) {
            case "battery":
                monitor = createBatteryLevelMonitor();
                break;
            case "disconnect":
                monitor = createDisconnectMonitor();
                break;
            case "statistics":
                monitor = createStatisticsMonitor();
                break;
            case "all":
                monitor = new CombinedKafkaMonitor(Arrays.asList(
                        createDisconnectMonitor(),
                        createBatteryLevelMonitor(),
                        createStatisticsMonitor()));
                break;
            default:
                throw new IllegalArgumentException("Cannot create unknown monitor " + commandType);
        }
        if (monitor == null) {
            throw new IllegalArgumentException("Monitor " + commandType + " is not configured.");
        }
        return monitor;
    }

    private KafkaMonitor createStatisticsMonitor() {
        SourceStatisticsMonitorConfig config = properties.getRadarProperties()
                .getStatisticsMonitor();

        if (config == null) {
            logger.warn("Statistics monitor is not configured. Cannot start it.");
            return null;
        }

        List<String> topics = config.getTopics();
        if (topics == null || topics.isEmpty()) {
            topics = Collections.singletonList("application_uptime");
        }

        return new SourceStatisticsMonitor(properties, topics, config.getOutputTopic());
    }

    private KafkaMonitor createBatteryLevelMonitor() throws IOException {
        BatteryMonitorConfig config = properties.getRadarProperties().getBatteryMonitor();

        if (config == null) {
            logger.warn("Battery level monitor is not configured. Cannot start it.");
            return null;
        }

        BatteryLevelMonitor.Status minLevel = BatteryLevelMonitor.Status.CRITICAL;
        EmailSender sender = getSender(config);
        Collection<String> topics = getTopics(config, "android_empatica_e4_battery_level");

        if (config.getLevel() != null) {
            String level = config.getLevel().toUpperCase(Locale.US);
            try {
                minLevel = BatteryLevelMonitor.Status.valueOf(level);
            } catch (IllegalArgumentException ex) {
                logger.warn("Minimum battery level {} is not recognized. "
                                + "Choose from {} instead. Using CRITICAL.",
                        level, Arrays.toString(BatteryLevelMonitor.Status.values()));
            }
        }
        long logInterval = config.getLogInterval();

        return new BatteryLevelMonitor(properties, topics, sender, minLevel, logInterval);
    }

    private KafkaMonitor createDisconnectMonitor()
            throws IOException {
        DisconnectMonitorConfig config = properties.getRadarProperties().getDisconnectMonitor();
        if (config == null) {
            logger.warn("Disconnect monitor is not configured. Cannot start it.");
            return null;
        }
        EmailSender sender = getSender(config);
        Collection<String> topics = getTopics(config, "android_empatica_e4_temperature");
        return new DisconnectMonitor(properties, topics, "disconnect_monitor", sender);
    }

    private EmailSender getSender(MonitorConfig config) throws IOException {
        if (config != null && config.getEmailAddress() != null) {
            return new EmailSender(config.getEmailHost(), config.getEmailPort(),
                    config.getEmailUser(),
                    config.getEmailAddress());
        }
        return null;
    }

    private Collection<String> getTopics(MonitorConfig config, String defaultTopic) {
        if (config != null && config.getTopics() != null) {
            return config.getTopics();
        } else {
            return Collections.singleton(defaultTopic);
        }
    }
}

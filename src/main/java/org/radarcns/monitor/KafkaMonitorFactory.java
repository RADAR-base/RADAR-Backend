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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.radarcns.config.EmailServerConfig;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.monitor.BatteryMonitorConfig;
import org.radarcns.config.monitor.DisconnectMonitorConfig;
import org.radarcns.config.monitor.EmailNotifyConfig;
import org.radarcns.config.monitor.InterventionMonitorConfig;
import org.radarcns.config.monitor.MonitorConfig;
import org.radarcns.monitor.intervention.InterventionMonitor;
import org.radarcns.util.EmailSenders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMonitorFactory {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMonitorFactory.class);

    private final RadarPropertyHandler properties;
    private final RadarBackendOptions options;
    private final EmailServerConfig emailServer;

    public KafkaMonitorFactory(RadarBackendOptions options,
            RadarPropertyHandler properties) {
        this.options = options;
        this.properties = properties;
        this.emailServer = properties.getRadarProperties().getEmailServerConfig();
    }

    public KafkaMonitor createMonitor() throws IOException {
        KafkaMonitor monitor;
        List<String> args = options.getSubCommandArgs();
        String commandType;
        if (args.isEmpty()) {
            commandType = "all";
        } else {
            commandType = args.get(0);
        }

        monitor = switch (commandType) {
            case "battery" -> createBatteryLevelMonitor();
            case "disconnect" -> createDisconnectMonitor();
            case "intervention" -> createInterventionMonitor();
            case "all" -> new CombinedKafkaMonitor(
                    Stream.of(createDisconnectMonitor(), createBatteryLevelMonitor(),
                            createInterventionMonitor()));
            default -> throw new IllegalArgumentException(
                    "Cannot create unknown monitor " + commandType);
        };
        if (monitor == null) {
            throw new IllegalArgumentException("Monitor " + commandType + " is not configured.");
        }
        return monitor;
    }

    private KafkaMonitor createInterventionMonitor() throws IOException {
        InterventionMonitorConfig config = properties.getRadarProperties().getInterventionMonitor();
        if (config == null) {
            logger.warn("Notification monitor is not configured. Cannot start it.");
            return null;
        }

        EmailSenders senders = createSenders(config.getEmailNotifyConfig());
        return new InterventionMonitor(config, properties, senders);
    }

    private KafkaMonitor createBatteryLevelMonitor() throws IOException {
        BatteryMonitorConfig config = properties.getRadarProperties().getBatteryMonitor();

        if (config == null) {
            logger.warn("Battery level monitor is not configured. Cannot start it.");
            return null;
        }

        BatteryLevelMonitor.Status minLevel = BatteryLevelMonitor.Status.CRITICAL;
        EmailSenders senders = createSenders(config.getEmailNotifyConfig());
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

        return new BatteryLevelMonitor(properties, topics, senders, minLevel, logInterval);
    }

    private KafkaMonitor createDisconnectMonitor()
            throws IOException {
        DisconnectMonitorConfig config = properties.getRadarProperties().getDisconnectMonitor();
        if (config == null) {
            logger.warn("Disconnect monitor is not configured. Cannot start it.");
            return null;
        }
        EmailSenders senders = createSenders(config.getEmailNotifyConfig());
        Collection<String> topics = getTopics(config, "android_empatica_e4_temperature");
        return new DisconnectMonitor(properties, topics, "disconnect_monitor", senders);
    }


    private EmailSenders createSenders(List<EmailNotifyConfig> notifyConfig) throws IOException {
        if (emailServer != null && notifyConfig != null) {
            return EmailSenders.parseConfig(emailServer, notifyConfig);
        } else {
            logger.warn("Monitor does not have email configured. "
                    + "Will not send email notifications.");
        }
        return null;
    }

    private Collection<String> getTopics(MonitorConfig config, String defaultTopic) {
        if (config != null && config.getTopics() != null) {
            return config.getTopics();
        } else {
            return List.of(defaultTopic);
        }
    }
}

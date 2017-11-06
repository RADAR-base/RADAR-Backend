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

import static org.radarcns.util.PersistentStateStore.measurementKeyToString;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.mail.MessagingException;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.key.MeasurementKey;
import org.radarcns.monitor.BatteryLevelMonitor.BatteryLevelState;
import org.radarcns.util.EmailSender;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors the battery level for any devices running empty. It will optionally notify someone when
 * a battery level is running low and when the battery level has returned to normal again.
 */
public class BatteryLevelMonitor extends
        AbstractKafkaMonitor<GenericRecord, GenericRecord, BatteryLevelState> {
    private static final Logger logger = LoggerFactory.getLogger(BatteryLevelMonitor.class);

    private final EmailSender sender;
    private final Status minLevel;
    private final long logInterval;
    private long messageNumber;

    /**
     * BatteryLevelMonitor constructor.
     * @param radar RADAR properties
     * @param topics topics to monitor, each of which has a "batteryLevel" value field
     * @param sender email sender for notifications, null if no notifications should be sent.
     * @param minLevel minimum battery level, below which a notification should be sent
     * @param logInterval every how many messages to log, 0 for no log messages
     */
    public BatteryLevelMonitor(RadarPropertyHandler radar, Collection<String> topics,
            EmailSender sender, Status minLevel, long logInterval) {
        super(radar, topics, "battery_monitors", "1", new BatteryLevelState());

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        this.sender = sender;
        this.minLevel = minLevel == null ? Status.CRITICAL : minLevel;
        this.logInterval = logInterval;
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> record) {
        try {
            MeasurementKey key = extractKey(record);
            float batteryLevel = extractBatteryLevel(record);
            float previousLevel = state.updateLevel(key, batteryLevel);

            if (logInterval > 0 && ((int) (messageNumber % logInterval)) == 0) {
                logger.info("Measuring battery level of record offset {} of {} with value {}",
                        record.offset(), key, record.value());
            }
            messageNumber++;

            if (batteryLevel <= Status.CRITICAL.getLevel()) {
                if (previousLevel > Status.CRITICAL.getLevel()) {
                    updateStatus(key, Status.CRITICAL);
                    logger.warn("Battery level of sensor {} of user {} is critically low: {}",
                            key.getSourceId(), key.getUserId(), record.value());
                }
            } else if (batteryLevel <= Status.LOW.getLevel()) {
                if (previousLevel > Status.LOW.getLevel()) {
                    updateStatus(key, Status.LOW);
                    logger.warn("Battery level of sensor {} of user {} is low: {}",
                            key.getSourceId(), key.getUserId(), record.value());
                }
            } else if (previousLevel <= Status.LOW.getLevel()) {
                // updateStatus(key, Status.NORMAL);
                logger.info("Battery of sensor {} of user {} is has returned to normal: {}",
                        key.getSourceId(), key.getUserId(), record.value());
            }
        } catch (IllegalArgumentException ex) {
            logger.error("Failed to process record {}", record, ex);
        }
    }

    private void updateStatus(MeasurementKey key, Status status) {
        if (sender == null) {
            return;
        }

        if (status.getLevel() <= minLevel.getLevel()) {
            try {
                sender.sendEmail("[RADAR-CNS] battery level low",
                        "The battery level of " + key + " is now " + status + ". "
                                + "Please ensure that it gets recharged.");
                logger.info("Sent battery level status message successfully");
            } catch (MessagingException mex) {
                logger.error("Failed to send battery level status message.", mex);
            }
        }
        if (status == Status.NORMAL) {
            try {
                sender.sendEmail("[RADAR-CNS] battery level returned to normal",
                        "The battery level of " + key + " has returned to normal. "
                                + "No further action is needed.");
                logger.info("Sent battery level status message successfully");
            } catch (MessagingException mex) {
                logger.error("Failed to send battery level status message.", mex);
            }
        }
    }

    private float extractBatteryLevel(ConsumerRecord<?, GenericRecord> record) {
        GenericRecord value = record.value();
        Field batteryField = value.getSchema().getField("batteryLevel");
        if (batteryField == null) {
            throw new IllegalArgumentException("Failed to process record with value type "
                    + value.getSchema() + " without batteryLevel field.");
        }
        Number batteryLevel = (Number) value.get(batteryField.pos());
        return batteryLevel.floatValue();
    }

    public static void main(String[] args) throws IOException {
        RadarPropertyHandler radarPropertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        radarPropertyHandler.load(null);

        BatteryLevelMonitor monitor = new BatteryLevelMonitor(radarPropertyHandler,
                Collections.singletonList("android_empatica_e4_battery_level"), null, null, -1);
        monitor.start();
    }

    /** Battery level status. */
    public enum Status {
        NORMAL(1.0f), LOW(0.2f), CRITICAL(0.05f), EMPTY(0f);

        private final float level;

        Status(float level) {
            this.level = level;
        }

        /** Battery level. */
        public float getLevel() {
            return this.level;
        }
    }

    /** Persist messages that have been sent. */
    public static class BatteryLevelState {
        private Map<String, Float> levels = new HashMap<>();

        public Map<String, Float> getLevels() {
            return levels;
        }

        /** Set the last battery levels found. */
        public void setLevels(Map<String, Float> levels) {
            this.levels = levels;
        }

        /** Update a single battery level. */
        public float updateLevel(MeasurementKey key, float level) {
            Float previousLevel = levels.put(measurementKeyToString(key), level);
            return previousLevel == null ? 1.0f : previousLevel;
        }
    }
}

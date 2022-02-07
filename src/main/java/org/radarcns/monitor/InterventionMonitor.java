package org.radarcns.monitor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.consumer.realtime.action.appserver.AppserverClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InterventionMonitor extends
            AbstractKafkaMonitor<GenericRecord, GenericRecord, InterventionMonitor.InterventionNotificationState> {
        private static final Logger logger = LoggerFactory.getLogger(BatteryLevelMonitor.class);

    private final AppserverClient appserverClient;


    public InterventionMonitor(RadarPropertyHandler radar, Collection<String> topics,
                               AppserverClient appserverClient ) {
        super(radar, topics, "intervention_monitor", "1", new InterventionNotificationState());

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configure(props);

        this.appserverClient = appserverClient;
    }

    @Override
    protected void evaluateRecord(ConsumerRecord<GenericRecord, GenericRecord> records) {

    }

    /** Persist messages that have been sent. */
    public static class InterventionNotificationState {
        private Map<String, List<NotificationLog>> notificationLog = new HashMap<>();

        public Map<String, List<NotificationLog>> getNotificationLog() {
            return notificationLog;
        }

        /** Set the last notification that has been sent. */
        public void setNotificationLog(Map<String, List<NotificationLog>> log) {
            this.notificationLog = log;
        }

        /** Update a single notification. */
        public void updateLog(String key, NotificationLog log) {
            notificationLog.get(key).add(log);
        }
    }

    class NotificationLog {
        private String name;
        private long timestamp;
        private static final char SEPARATOR = '#';

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return name + SEPARATOR + timestamp;
        }
    }
}

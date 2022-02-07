package org.radarcns.config.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.radarcns.config.AppServerConfig;

import java.util.List;

public class NotificationMonitorConfig {

    @JsonProperty("app_server")
    private AppServerConfig appServerConfig;

    // The list of intervention topics, which will be used to evaluate the conditions
    private List<String> topics;

    // List of notification configs and corresponding conditions to trigger
    List<PushNotificationConfig> notify;

    public AppServerConfig getAppServerConfig() {
        return appServerConfig;
    }

    public void setAppServerConfig(AppServerConfig appServerConfig) {
        this.appServerConfig = appServerConfig;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<PushNotificationConfig> getNotify() {
        return notify;
    }

    public void setNotify(List<PushNotificationConfig> notify) {
        this.notify = notify;
    }
}

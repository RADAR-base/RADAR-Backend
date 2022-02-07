package org.radarcns.config.monitor;

import java.util.List;

public class InterventionMonitorConfig {

    // The list of intervention topics, which will be used to evaluate the conditions
    private List<String> topics;

    // List of notification configs and corresponding conditions to trigger
    List<PushNotificationConfig> notify;
}

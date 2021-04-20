package org.radarcns.consumer.realtime;

import org.radarcns.config.realtime.ActionConfig;

public class ActionFactory {

    public static Action getActionFor(ActionConfig actionConfig) {
        switch (actionConfig.getName()) {
            case ActiveAppNotificationAction.NAME:
                return new ActiveAppNotificationAction(actionConfig);

            default:
                throw new IllegalArgumentException(
                        "The specified action with name " + actionConfig.getName() + " is " +
                                "not correct.");
        }
    }
}

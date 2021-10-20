package org.radarcns.consumer.realtime.action;

import java.io.IOException;
import java.net.MalformedURLException;
import javax.mail.internet.AddressException;
import org.radarcns.config.realtime.ActionConfig;

public class ActionFactory {

  public static Action getActionFor(ActionConfig actionConfig) {
    switch (actionConfig.getName()) {
      case ActiveAppNotificationAction.NAME:
        try {
          return new ActiveAppNotificationAction(actionConfig);
        } catch (MalformedURLException exc) {
          throw new IllegalArgumentException(
              "The supplied url config was incorrect. Please check.");
        }
      case EmailUserAction.NAME:
        try {
          return new EmailUserAction(actionConfig);
        } catch (AddressException | IOException e) {
          throw new IllegalArgumentException("The configuration was invalid. Please check.");
        }
      default:
        throw new IllegalArgumentException(
            "The specified action with name " + actionConfig.getName() + " is " + "not correct.");
    }
  }
}

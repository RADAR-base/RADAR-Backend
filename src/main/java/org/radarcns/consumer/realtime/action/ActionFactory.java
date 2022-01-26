package org.radarcns.consumer.realtime.action;

import java.io.IOException;
import java.net.MalformedURLException;
import javax.mail.internet.AddressException;
import org.radarcns.config.realtime.ActionConfig;

/**
 * Factory class for {@link Action}s. It instantiates actions based on the configuration provided
 * for the given consumer.
 */
@SuppressWarnings("PMD.ClassNamingConventions")
public final class ActionFactory {

  private ActionFactory() {}

  public static Action getActionFor(ActionConfig actionConfig) {
    switch (actionConfig.getName()) {
      case ActiveAppNotificationAction.NAME:
        try {
          return new ActiveAppNotificationAction(actionConfig);
        } catch (MalformedURLException exc) {
          throw new IllegalArgumentException(
              "The supplied url config was incorrect. Please check.", exc);
        }
      case EmailUserAction.NAME:
        try {
          return new EmailUserAction(actionConfig);
        } catch (AddressException | IOException e) {
          throw new IllegalArgumentException("The configuration was invalid. Please check.", e);
        }
      default:
        throw new IllegalArgumentException(
            "The specified action with name " + actionConfig.getName() + " is " + "not correct.");
    }
  }
}

package org.radarcns.consumer.realtime.action;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarbase.appserver.client.AppserverClientConfig;
import org.radarbase.appserver.client.MessagingType;
import org.radarcns.config.realtime.ActionConfig;
import org.radarbase.appserver.client.AppserverClient;
import org.radarcns.consumer.realtime.action.appserver.NotificationContentProvider;
import org.radarcns.consumer.realtime.action.appserver.ProtocolNotificationProvider;
import org.radarcns.consumer.realtime.action.appserver.ScheduleTimeStrategy;
import org.radarcns.consumer.realtime.action.appserver.SimpleTimeStrategy;
import org.radarcns.consumer.realtime.action.appserver.TimeOfDayStrategy;

/**
 * This action can be used to trigger a notification for the aRMT app and schedule a corresponding
 * questionnaire for the user to fill out. This can also work as an intervention mechanism in some
 * use-cases.
 */
public class ActiveAppNotificationAction extends ActionBase {

  public static final String NAME = "ActiveAppNotificationAction";
  private final String questionnaireName;
  private final String timeOfDay;
  private final AppserverClient appserverClient;
  private final MessagingType type;

  public ActiveAppNotificationAction(ActionConfig actionConfig) throws MalformedURLException {
    super(actionConfig);
    questionnaireName =
        (String) actionConfig.getProperties().getOrDefault("questionnaire_name", "ers");

    String appServerBaseUrl =
        (String)
            actionConfig
                .getProperties()
                .getOrDefault(
                    "appserver_base_url",
                    "https://radar-cns-platform.rosalind.kcl.ac.uk/appserver");

    timeOfDay = (String) actionConfig.getProperties().getOrDefault("time_of_day", null);

    String mpTokenUrl =
        (String)
            actionConfig
                .getProperties()
                .getOrDefault(
                    "management_portal_token_url",
                    "https://radar-cns-platform.rosalind.kcl.ac.uk/managementportal/api/ouath/token");
    type =
        MessagingType.valueOf(
            (String)
                actionConfig
                    .getProperties()
                    .getOrDefault("message_type", MessagingType.NOTIFICATIONS.toString()));

    String clientId =
        (String) actionConfig.getProperties().getOrDefault("client_id", "realtime_consumer");
    String clientSecret =
        (String) actionConfig.getProperties().getOrDefault("client_secret", "secret");

    AppserverClientConfig config = new AppserverClientConfig();
    config.setClientId(clientId);
    config.setClientSecret(clientSecret);
    config.appserverUrl(appServerBaseUrl);
    config.tokenUrl(mpTokenUrl);

    appserverClient = new AppserverClient(config);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Boolean executeFor(ConsumerRecord<?, ?> record)
      throws IllegalArgumentException, IOException {
    GenericRecord key = (GenericRecord) record.key();

    if (!(key.get("projectId") instanceof String)) {
      throw new IllegalArgumentException(
          "Cannot execute Action " + NAME + ". The projectId is not valid.");
    }

    if (!(key.get("userId") instanceof String)) {
      throw new IllegalArgumentException(
          "Cannot execute Action " + NAME + ". The userId is not valid.");
    }

    if (!(key.get("sourceId") instanceof String)) {
      throw new IllegalArgumentException(
          "Cannot execute Action " + NAME + ". The sourceId is not valid.");
    }
    String project = (String) key.get("projectId");
    String user = (String) key.get("userId");
    String source = (String) key.get("sourceId");

    ScheduleTimeStrategy timeStrategy;
    if (timeOfDay != null && !timeOfDay.isEmpty()) {
      // get timezone for the user and create the correct local time of the day
      timeStrategy = new TimeOfDayStrategy(timeOfDay, getUserTimezone(project, user));
    } else {
      // no time of the day provided, schedule now.
      timeStrategy = new SimpleTimeStrategy(5, ChronoUnit.MINUTES);
    }

    // create the notification in appserver
    NotificationContentProvider contentProvider =
        new ProtocolNotificationProvider.Builder()
            .setName(questionnaireName)
            .setScheduledTime(timeStrategy.getScheduledTime())
            .setSourceId(source)
            .build();

    String body;
    switch (type) {
      case NOTIFICATIONS:
        body = contentProvider.getNotificationMessage();
        break;
      case DATA:
        body = contentProvider.getDataMessage();
        break;
      default:
        throw new IllegalArgumentException(
            "The type must be in " + Arrays.toString(MessagingType.values()));
    }

    appserverClient.createMessage(project, user, type, body);
    return true;
  }

  private String getUserTimezone(String project, String user) throws IOException {
    return (String) appserverClient.getUserDetails(project, user).getOrDefault("timezone", "gmt");
  }

}

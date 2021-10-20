package org.radarcns.consumer.realtime.action;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.config.realtime.ActionConfig;
import org.radarcns.consumer.realtime.action.appserver.AppserverClient;
import org.radarcns.consumer.realtime.action.appserver.NotificationContentProvider;
import org.radarcns.consumer.realtime.action.appserver.ProtocolNotificationProvider;
import org.radarcns.consumer.realtime.action.appserver.ScheduleTimeStrategy;
import org.radarcns.consumer.realtime.action.appserver.SimpleTimeStrategy;
import org.radarcns.consumer.realtime.action.appserver.TimeOfDayStrategy;

public class ActiveAppNotificationAction implements Action {

  public static final String NAME = "ActiveAppNotificationAction";
  private final String questionnaireName;
  private final String timeOfDay;
  private final AppserverClient appserverClient;
  private final String type;

  public ActiveAppNotificationAction(ActionConfig actionConfig) throws MalformedURLException {
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
        (String)
            actionConfig
                .getProperties()
                .getOrDefault("type", MessagingType.NOTIFICATIONS.toString().toLowerCase());

    String clientId =
        (String) actionConfig.getProperties().getOrDefault("client_id", "realtime_consumer");
    String clientSecret =
        (String) actionConfig.getProperties().getOrDefault("client_secret", "secret");
    appserverClient = new AppserverClient(appServerBaseUrl, mpTokenUrl, clientId, clientSecret);
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
    String project = (String) key.get("projectId");

    if (!(key.get("userId") instanceof String)) {
      throw new IllegalArgumentException(
          "Cannot execute Action " + NAME + ". The userId is not valid.");
    }
    String user = (String) key.get("userId");

    if (!(key.get("sourceId") instanceof String)) {
      throw new IllegalArgumentException(
          "Cannot execute Action " + NAME + ". The sourceId is not valid.");
    }
    String source = (String) key.get("sourceId");

    ScheduleTimeStrategy timeStrategy;
    if (timeOfDay != null && !timeOfDay.isEmpty()) {
      // no time of the day provided, schedule now.
      timeStrategy = new TimeOfDayStrategy(timeOfDay, getUserTimezone(project, user));
    } else {
      // get timezone for the user and create the correct local time of the day
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
    if (type.equals(MessagingType.NOTIFICATIONS.toString().toLowerCase())) {
      body = contentProvider.getNotificationMessage();
    } else if (type.equals(MessagingType.DATA.toString().toLowerCase())) {
      body = contentProvider.getDataMessage();
    } else {
      throw new IllegalArgumentException(
          "The type must be in " + Arrays.toString(MessagingType.namesLowerCase));
    }

    appserverClient.createMessage(project, user, type, body);
    return true;
  }

  private String getUserTimezone(String project, String user) throws IOException {
    return (String) appserverClient.getUserDetails(project, user).getOrDefault("timezone", "gmt");
  }

  public enum MessagingType {
    NOTIFICATIONS,
    DATA;

    public static final String[] namesLowerCase =
        Arrays.stream(values()).map(t -> t.toString().toLowerCase()).toArray(String[]::new);
  }
}

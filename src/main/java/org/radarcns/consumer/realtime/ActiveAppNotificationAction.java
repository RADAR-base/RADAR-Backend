package org.radarcns.consumer.realtime;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.config.realtime.ActionConfig;

public class ActiveAppNotificationAction implements Action {

  public static final String NAME = "aRMTNotification";
  private final String questionnaireName;
  private final String appServerBaseUrl;

  public ActiveAppNotificationAction(ActionConfig actionConfig) {
    questionnaireName =
        (String) actionConfig.getProperties().getOrDefault("questionnaire_name", "ers");

    appServerBaseUrl =
        (String)
            actionConfig
                .getProperties()
                .getOrDefault(
                    "appserver_base_url", "http://radar-cns-platform.rosalind.kcl.ac.uk/appserver");
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

    if (!(key.get("subjectId") instanceof String)) {
      throw new IllegalArgumentException(
          "Cannot execute Action " + NAME + ". The subjectId is not valid.");
    }
    String user = (String) key.get("subjectId");

    Instant scheduledTime = Instant.now().plus(Duration.ofMinutes(5));

    return null;
  }
}

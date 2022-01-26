package org.radarcns.consumer.realtime.action.appserver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * The content provides a questionnaire's protocol block in the message to be added to the appserver
 * and for the aRMT app to parse and schedule the specified questionnaire. Supports both FCM
 * Notification and Data Messages.
 */
@SuppressWarnings("PMD")
public class ProtocolNotificationProvider implements NotificationContentProvider {

  static final String PROTOCOL_TEMPLATE =
      "{"
          + "     \"action\" : \"QUESTIONNAIRE_TRIGGER\",\n"
          + "     \"questionnaire\": {\n"
          + "        \"name\": \"%s\",\n"
          + "        \"showIntroduction\": false,\n"
          + "        \"showInCalendar\": true,\n"
          + "        \"order\": %d,\n"
          + "        \"referenceTimestamp\": \"%s\",\n"
          + "        \"questionnaire\": {\n"
          + "          \"repository\": \"%s\",\n"
          + "          \"name\": \"%s\",\n"
          + "          \"avsc\": \"%s\"\n"
          + "        },\n"
          + "        \"startText\": {\n"
          + "          \"en\": \"\"\n"
          + "        },\n"
          + "        \"endText\": {\n"
          + "          \"en\": \"Thank you for taking the time today.\"\n"
          + "        },\n"
          + "        \"warn\": {\n"
          + "          \"en\": \"\"\n"
          + "        },\n"
          + "        \"estimatedCompletionTime\": 1,\n"
          + "        \"protocol\": {\n"
          + "          \"repeatProtocol\": {\n"
          + "            \"unit\": \"min\",\n"
          + "            \"amount\": %d\n"
          + "          },\n"
          + "          \"repeatQuestionnaire\": {\n"
          + "            \"unit\": \"min\",\n"
          + "            \"unitsFromZero\": \n"
          + "              %s\n"
          + "          },\n"
          + "          \"reminders\": {\n"
          + "            \"unit\": \"day\",\n"
          + "            \"amount\": 0,\n"
          + "            \"repeat\": 0\n"
          + "          },\n"
          + "          \"completionWindow\": {\n"
          + "            \"unit\": \"day\",\n"
          + "            \"amount\": %d\n"
          + "          }\n"
          + "        }\n"
          + "      },\n"
          + "      \"metadata\": %s\n"
          + " }"
          + "}";

  static final String NOTIFICATION_TEMPLATE =
      "{\n"
          + "\t\"title\" : \"%s\",\n"
          + "\t\"body\": \"%s\",\n"
          + "\t\"ttlSeconds\": %d,\n"
          + "\t\"sourceId\": \"%s\",\n"
          + "\t\"type\": \"%s\",\n"
          + "\t\"sourceType\": \"aRMT\",\n"
          + "\t\"appPackage\": \"%s\",\n"
          + "\t\"scheduledTime\": \"%s\"\n"
          + "\t\"additionalData\": \"%s\"\n"
          + " }";

  static final String DATA_TEMPLATE =
      "{\n"
          + "\t\"ttlSeconds\": %d,\n"
          + "\t\"sourceId\": \"%s\",\n"
          + "\t\"sourceType\": \"aRMT\",\n"
          + "\t\"appPackage\": \"%s\",\n"
          + "\t\"scheduledTime\": \"%s\"\n"
          + "\t\"dataMap\": \"%s\"\n"
          + " }";

  private final String notificationMessage;
  private final String dataMessage;

  private ProtocolNotificationProvider(Builder builder) {

    String metadataJson;
    try {
      metadataJson = new ObjectMapper().writeValueAsString(builder.metadata);
    } catch (JsonProcessingException exc) {
      // could not process as map, use empty metadata
      metadataJson = "{}";
    }

    String protocolSpec =
        String.format(
            PROTOCOL_TEMPLATE,
            builder.name.toUpperCase(Locale.ROOT), // based on convention in protocol files
            builder.order,
            builder.referenceTimestamp,
            builder.repo,
            builder.name,
            builder.avsc.toString(),
            builder.repeatProtocolMinutes,
            Arrays.toString(builder.repeatQuestionnaireMinutes),
            builder.completionWindowMinutes,
            metadataJson);

    notificationMessage =
        String.format(
            NOTIFICATION_TEMPLATE,
            builder.notificationTitle,
            builder.notificationBody,
            builder.ttlSeconds,
            builder.sourceId,
            builder.name,
            builder.appPackage,
            builder.scheduledTime,
            protocolSpec);

    this.dataMessage =
        String.format(
            DATA_TEMPLATE,
            builder.ttlSeconds,
            builder.sourceId,
            builder.appPackage,
            builder.scheduledTime,
            protocolSpec);
  }

  @Override
  public String getDataMessage() {
    return dataMessage;
  }

  @Override
  public String getNotificationMessage() {
    return notificationMessage;
  }

  @SuppressWarnings("PMD.ClassNamingConventions")
  public enum AllowedTypes {
    questionnaire,
    notification
  }

  public static class Builder {

    private static final String REPO =
        "https://raw.githubusercontent.com/RADAR-CNS/RADAR-REDCap-aRMT-Definitions/master/questionnaires/";

    // For the data message
    private Instant referenceTimestamp;
    private String repo;
    private int order;
    private String name;
    private AllowedTypes avsc = AllowedTypes.questionnaire;
    private Long repeatProtocolMinutes = 9999999999L; // a lot of years, it will not repeat.
    private Long[] repeatQuestionnaireMinutes = new Long[] {0L}; // Immediately scheduled once.
    private Long completionWindowMinutes = 24 * 60L; // 1day
    private Map<String, String> metadata = new HashMap<>();

    // For the notification message
    private String notificationTitle = "Questionnaire Time";
    private String notificationBody = "Urgent Questionnaire Pending. Please complete now.";
    private int ttlSeconds;
    private Instant scheduledTime;
    private String sourceId;
    private String appPackage = "org.phidatalab.radar_armt";

    public Builder() {
      repo = REPO;
    }

    public Builder(String repo) {
      this.repo = repo;
    }

    public Builder setRepo(String repo) {
      this.repo = repo;
      return this;
    }

    public Builder setOrder(int order) {
      this.order = order;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setAvsc(String avsc) {
      this.avsc = AllowedTypes.valueOf(avsc);
      return this;
    }

    public Builder setRepeatProtocolMinutes(Long repeatProtocolMinutes) {
      this.repeatProtocolMinutes = repeatProtocolMinutes;
      return this;
    }

    public Builder setRepeatQuestionnaireMinutes(Long[] repeatQuestionnaireMinutes) {
      this.repeatQuestionnaireMinutes = repeatQuestionnaireMinutes;
      return this;
    }

    public Builder setCompletionWindowMinutes(Long completionWindowMinutes) {
      this.completionWindowMinutes = completionWindowMinutes;
      return this;
    }

    public Builder setMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder setNotificationTitle(String notificationTitle) {
      this.notificationTitle = notificationTitle;
      return this;
    }

    public Builder setNotificationBody(String notificationBody) {
      this.notificationBody = notificationBody;
      return this;
    }

    public Builder setTtlSeconds(int ttlSeconds) {
      this.ttlSeconds = ttlSeconds;
      return this;
    }

    public Builder setScheduledTime(Instant scheduledTime) {
      this.scheduledTime = scheduledTime;
      return this;
    }

    public Builder setSourceId(String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public Builder setAppPackage(String appPackage) {
      this.appPackage = appPackage;
      return this;
    }

    public Builder setReferenceTimestamp(Instant referenceTimestamp) {
      this.referenceTimestamp = referenceTimestamp;
      return this;
    }

    public ProtocolNotificationProvider build() throws IllegalArgumentException {

      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Name is a required parameter");
      }

      if (scheduledTime == null) {
        throw new IllegalArgumentException("Scheduled Time cannot be null.");
      }

      if (sourceId == null || sourceId.isEmpty()) {
        throw new IllegalArgumentException("SourceId must be provided.");
      }

      if (referenceTimestamp == null) {
        referenceTimestamp = scheduledTime;
      }

      return new ProtocolNotificationProvider(this);
    }
  }
}

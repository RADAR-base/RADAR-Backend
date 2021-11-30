package org.radarcns.consumer.realtime.action;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.config.realtime.ActionConfig;
import org.radarcns.util.EmailSender;

/**
 * This action can be used to trigger an email to the user. Currently, it just notifies that the
 * conditions evaluated to true and provides some context. This is useful for project admins but can
 * be modified to also work as an intervention mechanism in some use-cases.
 */
public class EmailUserAction extends ActionBase {

  public static final String NAME = "EmailUserAction";
  private final EmailSender emailSender;
  private final String customTitle;
  private final String customBody;

  public EmailUserAction(ActionConfig actionConfig) throws AddressException, IOException {
    super(actionConfig);
    Map<String, Object> props = actionConfig.getProperties();
    this.emailSender =
        new EmailSender(
            (String) props.getOrDefault("host", "localhost"),
            (Integer) props.getOrDefault("port", 25),
            (String) props.getOrDefault("from", "admin@radarbase.org"),
            (List<String>) props.getOrDefault("email_addresses", new ArrayList<String>()));
    customTitle = (String) props.getOrDefault("title", null);
    customBody = (String) props.getOrDefault("body", null);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Boolean executeFor(ConsumerRecord<?, ?> record) {
    String title;
    title =
        Objects.requireNonNullElseGet(
            customTitle,
            () ->
                "Conditions triggered the action "
                    + NAME
                    + " for user "
                    + ((GenericRecord) record.key()).get("userId")
                    + " from topic "
                    + record.topic());
    String body;
    body =
        Objects.requireNonNullElseGet(
            customBody,
            () ->
                "Record: \n"
                    + record.value().toString()
                    + "\n\nTimestamp: "
                    + Instant.now()
                    + "\nKey: "
                    + record.key().toString());

    try {
      this.emailSender.sendEmail(title, body);
      return true;
    } catch (MessagingException e) {
      e.printStackTrace();
      return false;
    }
  }
}

package org.radarcns.consumer.realtime.action;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.consumer.realtime.Grouping;

/**
 * An action can be defined as any process that needs to take place when data is received and all
 * the {@link org.radarcns.consumer.realtime.condition.Condition}s have evaluated to true. It can be
 * emailing someone or just logging something.
 *
 * <p>See {@link ActiveAppNotificationAction}, {@link EmailUserAction}
 */
public interface Action extends Grouping {

  String getName();

  Boolean executeFor(ConsumerRecord<?, ?> record) throws IllegalArgumentException, IOException;

  default Boolean run(ConsumerRecord<?, ?> record) throws IllegalArgumentException, IOException {
    return evaluateProject(record) && executeFor(record);
  }
}

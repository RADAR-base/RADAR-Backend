package org.radarcns.consumer.realtime.condition;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.radarcns.consumer.realtime.Grouping;

/**
 * A condition can be defined as any predicate on the incoming data that must be true before the
 * {@link org.radarcns.consumer.realtime.action.Action}s can be triggered.
 */
public interface Condition extends Grouping {

  String getName();

  Boolean isTrueFor(ConsumerRecord<?, ?> record) throws IOException;

  default Boolean evaluate(ConsumerRecord<?, ?> record) throws IOException {
    return evaluateProject(record) && isTrueFor(record);
  }
}
package org.radarcns.consumer.realtime.condition;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Condition {

    String getName();

    Boolean isTrueFor(ConsumerRecord<?, ?> record) throws IOException;
}

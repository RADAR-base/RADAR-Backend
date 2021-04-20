package org.radarcns.consumer.realtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;

interface Condition {

    String getName();

    Boolean isTrueFor(ConsumerRecord<?, ?> record);
}

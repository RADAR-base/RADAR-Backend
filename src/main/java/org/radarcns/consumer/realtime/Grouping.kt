package org.radarcns.consumer.realtime;

import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Grouping {

  List<String> getProjects();

  default Boolean evaluateProject(ConsumerRecord<?, ?> record) throws IOException {
    return getProjects() == null
        || getProjects().isEmpty()
        || getProjects().contains((String) ((GenericRecord) record.key()).get("projectId"));
  }
}

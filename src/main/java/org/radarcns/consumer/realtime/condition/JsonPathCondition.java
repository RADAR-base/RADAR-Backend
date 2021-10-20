package org.radarcns.consumer.realtime.condition;

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Uses https://github.com/json-path/JsonPath to evaluate json expressions directly in the record
 * making this condition a generic one for simple use cases such as predicates and comparisons for a
 * field in the json record.
 */
public abstract class JsonPathCondition implements Condition {

  protected Boolean evaluateJsonPath(ConsumerRecord<?, ?> record, String jsonPath)
      throws IOException {
    // JsonPath expressions always return a List.
    List<?> result;
    try {
      result = JsonPath.parse(record.value()).read(jsonPath);
    } catch (ClassCastException exc) {
      throw new IOException(
          "The provided json path does not seem to contain an expression. Make sure it"
              + " contains an expression. Docs: https://github.com/json-path/JsonPath");
    }

    // At least one result matches the condition
    return result != null && result.size() > 0;
  }
}

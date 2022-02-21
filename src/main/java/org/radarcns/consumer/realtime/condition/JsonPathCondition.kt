package org.radarcns.consumer.realtime.condition

import com.jayway.jsonpath.JsonPath
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.config.realtime.ConditionConfig
import java.io.IOException

/**
 * Uses https://github.com/json-path/JsonPath to evaluate json expressions directly in the record
 * making this condition a generic one for simple use cases such as predicates and comparisons for a
 * field in the json record.
 */
abstract class JsonPathCondition(config: ConditionConfig) : ConditionBase(config) {
    @Throws(IOException::class)
    protected fun evaluateJsonPath(
            record: ConsumerRecord<*, *>,
            jsonPath: String?, rootKey: String? = null): Boolean {
        // JsonPath expressions always return a List.
        val result: List<*>? = try {

            val valueToParse = if (rootKey != null) {
                (record.value() as GenericRecord).get(rootKey) as String
            } else {
                (record.value() as GenericRecord).toString()
            }

            JsonPath.parse(valueToParse).read(jsonPath)
        } catch (exc: ClassCastException) {
            throw IOException("The provided json path does not seem to contain an expression. Make sure it"
                    + " contains an expression. Docs: https://github.com/json-path/JsonPath", exc)
        }

        // At least one result matches the condition
        return !result.isNullOrEmpty()
    }
}
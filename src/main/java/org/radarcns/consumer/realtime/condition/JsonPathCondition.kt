package org.radarcns.consumer.realtime.condition

import kotlin.Throws
import java.io.IOException
import org.radarcns.config.realtime.ConditionConfig
import org.radarcns.consumer.realtime.condition.LocalJsonPathCondition
import java.lang.IllegalArgumentException
import org.radarcns.consumer.realtime.condition.ConditionBase
import com.jayway.jsonpath.JsonPath
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.lang.ClassCastException
import org.radarcns.consumer.realtime.condition.JsonPathCondition

/**
 * Uses https://github.com/json-path/JsonPath to evaluate json expressions directly in the record
 * making this condition a generic one for simple use cases such as predicates and comparisons for a
 * field in the json record.
 */
abstract class JsonPathCondition(config: ConditionConfig) : ConditionBase(config) {
    @Throws(IOException::class)
    protected fun evaluateJsonPath(record: ConsumerRecord<*, *>, jsonPath: String?): Boolean {
        // JsonPath expressions always return a List.
        val result: List<*>? = try {
            JsonPath.parse(record.value()).read(jsonPath)
        } catch (exc: ClassCastException) {
            throw IOException("The provided json path does not seem to contain an expression. Make sure it"
                    + " contains an expression. Docs: https://github.com/json-path/JsonPath", exc)
        }

        // At least one result matches the condition
        return result != null && result.isNotEmpty()
    }
}
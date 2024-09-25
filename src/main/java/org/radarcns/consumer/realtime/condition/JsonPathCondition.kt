package org.radarcns.consumer.realtime.condition

import com.jayway.jsonpath.JsonPath
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.config.realtime.ConditionConfig
import org.slf4j.LoggerFactory
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
                (record.value() as GenericRecord?)?.get(rootKey)?.toString() ?: return false
            } else {
                (record.value() as GenericRecord?)?.toString() ?: return false
            }

            logger.debug("value: $valueToParse")

            JsonPath.parse(valueToParse).read(jsonPath)
        } catch (exc: ClassCastException) {
            throw IOException("The provided json path does not seem to contain an expression. Make sure it"
                    + " contains an expression. Docs: https://github.com/json-path/JsonPath", exc)
        }

        logger.debug("JsonPath result: $result")

        // At least one result matches the condition
        return !result.isNullOrEmpty()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(JsonPathCondition::class.java)
    }
}
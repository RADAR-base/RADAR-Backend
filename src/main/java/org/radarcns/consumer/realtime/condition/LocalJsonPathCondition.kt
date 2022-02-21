package org.radarcns.consumer.realtime.condition

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.config.realtime.ConditionConfig
import java.io.IOException

/**
 * Reads the JsonPath specification from the configuration file, provided to this class in [ ].
 */
class LocalJsonPathCondition(
        conditionConfig: ConditionConfig,
        override val name: String = NAME,
) : JsonPathCondition(conditionConfig) {
    private val jsonPath: String?

    @Throws(IOException::class)
    override fun isTrueFor(record: ConsumerRecord<*, *>?): Boolean {
        return evaluateJsonPath(record!!, jsonPath)
    }

    companion object {
        const val NAME = "LocalJsonPathCondition"
    }

    init {
        jsonPath = requireNotNull(
                conditionConfig.properties?.let { it["jsonpath"] as String? }
        ) {
            ("The 'jsonpath' property needs to be specified when "
                    + "using the LocalJsonPathCondition.")
        }
    }
}
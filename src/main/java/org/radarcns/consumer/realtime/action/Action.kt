package org.radarcns.consumer.realtime.action

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.consumer.realtime.Grouping
import java.io.IOException

/**
 * An action can be defined as any process that needs to take place when data is received and all
 * the [org.radarcns.consumer.realtime.condition.Condition]s have evaluated to true. It can be
 * emailing someone or just logging something.
 *
 *
 * See [ActiveAppNotificationAction], [EmailUserAction]
 */
interface Action : Grouping {
    val name: String

    @Throws(IllegalArgumentException::class, IOException::class)
    fun executeFor(record: ConsumerRecord<*, *>?): Boolean

    @Throws(IllegalArgumentException::class, IOException::class)
    fun run(record: ConsumerRecord<*, *>?): Boolean {
        return evaluateProject(record) && executeFor(record)
    }
}
package org.radarcns.consumer.realtime.condition

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.consumer.realtime.Grouping
import java.io.IOException

/**
 * A condition can be defined as any predicate on the incoming data that must be true before the
 * [org.radarcns.consumer.realtime.action.Action]s can be triggered.
 */
interface Condition : Grouping {
    val name: String

    @Throws(IOException::class)
    fun isTrueFor(record: ConsumerRecord<*, *>?): Boolean

    @Throws(IOException::class)
    fun evaluate(record: ConsumerRecord<*, *>?): Boolean {
        return evaluateProject(record) && isTrueFor(record)
    }
}
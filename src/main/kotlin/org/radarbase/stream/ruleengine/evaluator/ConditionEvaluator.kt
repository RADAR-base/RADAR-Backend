package org.radarbase.stream.ruleengine.evaluator

import kotlinx.io.IOException
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.api.Record

/**
 * TODO update package ref
 * A condition can be defined as any predicate on the incoming data that must be true before the
 * [org.radarbase.consumer.realtime.action.Action]s can be triggered.
 */
interface ConditionEvaluator {

    @Throws(IOException::class)
    fun isTrueFor(record: Record<GenericRecord, GenericRecord>, expression: String): Boolean

}
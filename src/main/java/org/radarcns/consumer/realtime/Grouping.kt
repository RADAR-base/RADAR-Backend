package org.radarcns.consumer.realtime

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.kafka.ObservationKey
import java.io.IOException

interface Grouping {
    val projects: List<String>?
    val subjects: List<String>?

    @Throws(IOException::class)
    fun evaluateProject(record: ConsumerRecord<*, *>?): Boolean {
        return record?.key()?.let {

            val projectEval = if (!projects.isNullOrEmpty()) {
                val pidKey: String = findKey(record, PROJECT_ID_KEYS)
                        ?: // No valid project id key found in the record
                        return false
                return projects!!.contains((record.key() as GenericRecord)[pidKey].toString())
            } else true

            val subjectEval = if (!subjects.isNullOrEmpty()) {
                val uidKey: String = findKey(record, USER_ID_KEYS)
                        ?: // No valid user id key found in the record
                        return false
                return subjects!!.contains((record.key() as GenericRecord)[uidKey].toString())
            } else true

            return projectEval && subjectEval
        } ?: false // No valid key found in the record
    }

    companion object {
        val PROJECT_ID_KEYS = arrayOf("projectId", "PROJECTID", "project_id", "PROJECT_ID")
        val USER_ID_KEYS = arrayOf("userId", "USERID", "user_id", "USER_ID")
        val SOURCE_ID_KEYS = arrayOf("sourceId", "SOURCEID", "source_id", "SOURCE_ID")
        val TIME_VALUE_KEYS = arrayOf("time", "TIME")

        val objectMapper: ObjectMapper = ObjectMapper()
                .registerModule(KotlinModule.Builder()
                        .enable(KotlinFeature.NullIsSameAsDefault)
                        .enable(KotlinFeature.NullToEmptyCollection)
                        .enable(KotlinFeature.NullToEmptyMap)
                        .build())
                .registerModule(JavaTimeModule())

        fun findKey(record: ConsumerRecord<*, *>?, keys: Array<String>): String? {
            return record?.key()?.let {
                val key = it as GenericRecord
                keys.find { k -> key.hasField(k) }
            }
        }

        fun getKeys(record: ConsumerRecord<*, *>?): ObservationKey? {
            return record?.key()?.let { k ->
                val key = k as GenericRecord

                val pidKey: String = findKey(record, PROJECT_ID_KEYS)
                        ?: throw IllegalArgumentException("No project id found in key")

                val uidKey: String = findKey(record, USER_ID_KEYS)
                        ?: throw IllegalArgumentException("No user id found in key")

                val sidKey: String? = findKey(record, SOURCE_ID_KEYS)

                val project = key[pidKey].toString()
                val user = key[uidKey].toString()
                val source = if (sidKey != null) key[sidKey].toString() else null

                ObservationKey(project, user, source)
            }
        }

        fun getTime(record: ConsumerRecord<*, *>?): Long {
            return record?.value()?.let { v ->
                val value = v as GenericRecord
                val key = TIME_VALUE_KEYS.find { k -> value.hasField(k) }
                        ?: throw IllegalArgumentException("No time found in key")
                value[key].toString().toDouble().toLong()
            } ?: throw IllegalArgumentException("Time could  not be parsed from record")
        }
    }
}
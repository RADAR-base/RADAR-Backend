package org.radarcns.consumer.realtime

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.io.IOException

interface Grouping {
    val projects: List<String>?
    val subjects: List<String>?

    @Throws(IOException::class)
    fun evaluateProject(record: ConsumerRecord<*, *>?): Boolean {
        return record?.key()?.let {

            val key = it as GenericRecord

            val projectEval = if(!projects.isNullOrEmpty()) {
                val pidKey: String = PROJECT_ID_KEYS.mapNotNull { p -> key.get(p) as String? }.firstOrNull()
                        ?: // No valid project id key found in the record
                        return false
                return projects!!.contains((record.key() as GenericRecord)[pidKey] as String)
            } else true

            val subjectEval = if(!subjects.isNullOrEmpty()) {
                val uidKey: String = USER_ID_KEYS.mapNotNull { p -> key.get(p) as String? }.firstOrNull()
                        ?: // No valid user id key found in the record
                        return false
                return subjects!!.contains((record.key() as GenericRecord)[uidKey] as String)
            } else true

            return projectEval && subjectEval
        } ?: false // No valid key found in the record
    }

    companion object {
        val PROJECT_ID_KEYS = arrayOf("projectId", "PROJECTID", "project_id", "PROJECT_ID")
        val USER_ID_KEYS = arrayOf("userId", "USERID", "user_id", "USER_ID")
    }
}
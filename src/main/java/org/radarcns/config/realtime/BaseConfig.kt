package org.radarcns.config.realtime

interface BaseConfig {
    val name: String
    val properties: Map<String, Any>?
    val projects: List<String>?
    val subjects: List<String>?
    val projectIdField: String?
    val subjectIdField: String?
    val sourceIdField: String?
    val timeField: String?
}
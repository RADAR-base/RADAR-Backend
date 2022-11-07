package org.radarcns.config.realtime

data class ActionConfig(
        override val name: String,
        override val properties: Map<String, Any>? = null,
        override val projects: List<String>? = null,
        override val subjects: List<String>? = null,
        override val projectIdField: String? = null,
        override val subjectIdField: String? = null,
        override val sourceIdField: String? = null,
        override val timeField: String? = null,
): BaseConfig
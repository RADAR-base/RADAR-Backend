package org.radarcns.config.realtime

data class ActionConfig(
        val name: String,
        val properties: Map<String, Any>? = null,
        val projects: List<String>? = null,
        val subjects: List<String>? = null,
)
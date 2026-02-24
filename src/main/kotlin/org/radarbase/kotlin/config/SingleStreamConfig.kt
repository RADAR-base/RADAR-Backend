package org.radarbase.kotlin.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import org.radarbase.kotlin.config.RadarPropertyHandler.Priority
import java.util.*

open class SingleStreamConfig {
    @JsonProperty("class")
    var streamClass: Class<*>? = null

    @JsonProperty
    var properties: Map<String, String> = emptyMap()

    @JsonProperty
    var priority: Priority = Priority.NORMAL

    @JsonSetter("priority")
    protected fun setPriority(priority: String) {
        this.priority = Priority.valueOf(priority.uppercase(Locale.US))
    }

    fun setDefaultPriority(priority: Priority) {
        if (this.priority == Priority.NORMAL) {
            this.priority = priority
        }
    }
}

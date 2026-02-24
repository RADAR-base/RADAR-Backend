package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import java.util.*

open class SingleStreamConfig {
    @JsonProperty("class")
    var streamClass: Class<*>? = null

    @JsonProperty
    var properties: Map<String, String> = emptyMap()

    @JsonProperty
    var priority: RadarPropertyHandler.Priority = RadarPropertyHandler.Priority.NORMAL

    @JsonSetter("priority")
    protected fun setPriority(priority: String) {
        this.priority = RadarPropertyHandler.Priority.valueOf(priority.uppercase(Locale.US))
    }

    fun setDefaultPriority(priority: RadarPropertyHandler.Priority) {
        if (this.priority == RadarPropertyHandler.Priority.NORMAL) {
            this.priority = priority
        }
    }
}

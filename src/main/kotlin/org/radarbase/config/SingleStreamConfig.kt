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
    var priority: RadarConfigHandler.Priority = RadarConfigHandler.Priority.NORMAL

    @JsonSetter("priority")
    protected fun setPriority(priority: String) {
        this.priority = RadarConfigHandler.Priority.valueOf(priority.uppercase(Locale.US))
    }

    fun setDefaultPriority(priority: RadarConfigHandler.Priority) {
        if (this.priority == RadarConfigHandler.Priority.NORMAL) {
            this.priority = priority
        }
    }
}

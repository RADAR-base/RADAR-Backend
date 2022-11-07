package org.radarcns.config.monitor

import com.fasterxml.jackson.annotation.JsonProperty

data class ThresholdAdaptationConfig(
    @JsonProperty("adjust_value")
    val adjustValue: Float = 0.01f,
    @JsonProperty("optimal_interventions")
    val optimalInterventions: Int = 3,
)

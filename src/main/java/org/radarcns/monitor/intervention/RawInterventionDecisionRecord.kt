package org.radarcns.monitor.intervention

import com.fasterxml.jackson.annotation.JsonProperty

data class RawInterventionDecisionRecord(
    @JsonProperty("DECISION")
    val decision: Boolean,
    @JsonProperty("NAME")
    val name: String? = null,
    @JsonProperty("EXCEPTION")
    val exception: String? = null,
)

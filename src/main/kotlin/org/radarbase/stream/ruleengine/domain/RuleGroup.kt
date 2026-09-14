package org.radarbase.stream.ruleengine.domain

import org.radarbase.config.intervention.InterventionConfig

data class RuleGroup(
    val rules: MutableMap<RuleKey, InterventionConfig> = mutableMapOf(),
)

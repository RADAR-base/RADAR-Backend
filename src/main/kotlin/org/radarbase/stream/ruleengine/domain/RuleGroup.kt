package org.radarbase.stream.ruleengine.domain

import org.radarbase.config.intervention.InterventionConfig

// Keyed by RuleKey.toStoreKey() rather than RuleKey itself: Jackson serializes a non-String map
// key via toString() but has no default KeyDeserializer for an arbitrary data class, so
// store.get() would throw once a scope has more than one stored rule.
data class RuleGroup(
    val rules: MutableMap<String, InterventionConfig> = mutableMapOf(),
)

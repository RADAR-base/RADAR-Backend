package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.radarbase.config.intervention.ConditionConfig
import org.radarbase.config.intervention.InterventionConfig
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.slf4j.LoggerFactory

class RuleGroupProcessor(
    val globalStoreName: String,
) : Processor<RuleKey, InterventionConfig, Void, Void> {

    private lateinit var store: KeyValueStore<String, RuleGroup>

    override fun init(context: ProcessorContext<Void, Void>) {
        store = context.getStateStore(globalStoreName)
    }

    override fun process(record: Record<RuleKey, InterventionConfig>) {
        val key = record.key() ?: return
        val value = record.value()

        // Key to collect all rules for a given topic and scope. Examples:
        value.conditionConfigs.forEach { conditionConfig ->

            logger.debug("Processing condition {} for rule {}", conditionConfig, key)

            conditionConfig.toScopeKeys().forEach { scopeKey ->

                val currentGroup = store.get(scopeKey) ?: RuleGroup()
                val groupRules = currentGroup.rules

                // When tombstone is received.
                if (value == null) {
                    groupRules.remove(key)
                    logger.debug("Removed condition key {} for scope {}", key, scopeKey)
                }
                if (value != null) {
                    groupRules[key] = value
                    logger.debug("Added condition key {} for scope {}", key, scopeKey)
                }

                if (groupRules.isEmpty()) {
                    store.delete(scopeKey)
                    logger.debug("Removed all conditions for scope {}", scopeKey)
                } else {
                    store.put(scopeKey, RuleGroup(groupRules))
                    logger.debug("Updating conditions for scope {}", scopeKey)
                }
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RuleGroupProcessor::class.java)
    }
}

fun ConditionConfig.toScopeKeys(): List<String> {
    if (!this.subjects.isNullOrEmpty()) {
        return this.subjects.map { "${ScopePrefix.PROJECT}$it" }
    }
    if (!this.projects.isNullOrEmpty()) {
        return this.projects.map { "${ScopePrefix.USER}$it" }
    }
    return listOf(ScopePrefix.GLOBAL.scope)
}

enum class ScopePrefix(val scope: String) {
    GLOBAL("global"), PROJECT("project."), USER("user."),
}

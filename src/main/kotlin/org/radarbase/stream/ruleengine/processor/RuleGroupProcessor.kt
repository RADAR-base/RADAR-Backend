package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.RuleValue

class RuleGroupProcessor(
    val globalStoreName: String,
) : Processor<RuleKey, RuleValue, Void, Void> {
    private lateinit var store: KeyValueStore<String, RuleGroup>

    override fun init(context: ProcessorContext<Void, Void>) {
        store = context.getStateStore(globalStoreName)
    }

    override fun process(record: Record<RuleKey, RuleValue>) {
        val key = record.key()
        val value = record.value()
        val lookupKey = "${key.topicName}:${key.project}"

        val aggregate = store.get(lookupKey) ?: RuleGroup()
        val newRules = aggregate.rules.toMutableList()

        newRules.removeIf { it.first == key }
        if (value != null) {
            newRules.add(key to value)
        }

        if (newRules.isEmpty()) {
            store.delete(lookupKey)
        } else {
            store.put(lookupKey, RuleGroup(newRules))
        }
    }
}

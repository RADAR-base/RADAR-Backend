package org.radarbase.stream.ruleengine.processor

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.state.KeyValueIterator
import org.apache.kafka.streams.state.KeyValueStore
import org.radarbase.stream.ruleengine.domain.RuleGroup

class FakeRuleGroupStore : KeyValueStore<String, RuleGroup> {
    private val data = linkedMapOf<String, RuleGroup>()

    fun putDirect(key: String, group: RuleGroup) {
        data[key] = group
    }

    override fun get(key: String): RuleGroup? = data[key]

    override fun range(from: String?, to: String?): KeyValueIterator<String, RuleGroup> =
        throw UnsupportedOperationException("range is not supported by FakeRuleGroupStore")

    override fun all(): KeyValueIterator<String, RuleGroup> {
        val snapshot = data.entries.map { KeyValue(it.key, it.value) }.iterator()
        return object : KeyValueIterator<String, RuleGroup> {
            override fun close() {}
            override fun peekNextKey(): String = throw UnsupportedOperationException()
            override fun hasNext(): Boolean = snapshot.hasNext()
            override fun next(): KeyValue<String, RuleGroup> = snapshot.next()
            override fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    override fun approximateNumEntries(): Long = data.size.toLong()

    override fun put(key: String, value: RuleGroup) {
        data[key] = value
    }

    override fun putIfAbsent(key: String, value: RuleGroup): RuleGroup? {
        val existing = data[key]
        if (existing == null) data[key] = value
        return existing
    }

    override fun putAll(entries: MutableList<KeyValue<String, RuleGroup>>) {
        entries.forEach { data[it.key] = it.value }
    }

    override fun delete(key: String): RuleGroup? = data.remove(key)

    override fun name(): String = "fake-rule-group-store"

    override fun init(context: ProcessorContext, root: StateStore) {}

    override fun flush() {}

    override fun close() {}

    override fun persistent(): Boolean = false

    override fun isOpen(): Boolean = true
}

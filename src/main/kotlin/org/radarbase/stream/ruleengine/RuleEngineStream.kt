package org.radarbase.stream.ruleengine

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.Stores
import org.radarbase.config.GlobalStoreConfig
import org.radarbase.stream.AbstractStreamWorker
import org.radarbase.stream.StreamDefinition
import org.radarbase.stream.ruleengine.domain.ActionConfig
import org.radarbase.stream.ruleengine.domain.RuleGroup
import org.radarbase.stream.ruleengine.domain.RuleKey
import org.radarbase.stream.ruleengine.domain.RuleValue
import org.radarbase.stream.ruleengine.processor.RuleGroupProcessor
import org.radarbase.stream.ruleengine.processor.RuleProcessor
import org.radarbase.stream.ruleengine.serde.JsonSerde
import org.radarbase.topic.KafkaTopic
import org.radarbase.util.getStreamProperties

/**
 * Definition of Kafka Stream for aggregating data collected by Empatica E4 Accelerometer sensor.
 */
class RuleEngineStream : AbstractStreamWorker() {

    val ruleGroupSerde = JsonSerde(RuleGroup::class.java)
    val ruleKeySerde = JsonSerde(RuleKey::class.java)
    val ruleValueSerde = JsonSerde(RuleValue::class.java)
    val actionConfigSerde = JsonSerde(ActionConfig::class.java)

    /*
      Create Stream Topologies based on StreamDefinitions
    */
    override fun createStreams(): List<KafkaStreams>? {
        return getStreamDefinitions().map { def ->
            val storeConfig = def.globalStoreConfig!!
            val storeBuilder =  Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeConfig.storeName),
                Serdes.String(),
                ruleGroupSerde,
            )
            val builder = StreamsBuilder()
            builder.addGlobalStore(
                storeBuilder,
                storeConfig.topic.name,
                Consumed.with(ruleKeySerde, ruleValueSerde),
                ProcessorSupplier { RuleGroupProcessor(storeConfig.storeName) }
            )
            builder.stream<GenericRecord, GenericRecord>(def.inputTopic.name)
                .process(ProcessorSupplier { RuleProcessor(storeConfig.storeName) })
                .to({ _, actionConfig, _ -> actionConfig.topic }, Produced.with(ruleKeySerde, actionConfigSerde))
            val properties = getStreamProperties(
                this.javaClass, def, config, allConfig, kafkaProperty, null)
            return@map KafkaStreams(builder.build(), properties)
        }.toList()
    }

    override fun doCleanup() {
        TODO("Not yet implemented")
    }

    /*
        Add StreamDefinition for the RuleEngineStream
     */
    override fun initialize() {
        assert(config.properties.containsKey("input_topic")) { "Input topic name not specified in stream properties" }
        assert(config.properties.containsKey("output_topic")) { "Output topic name not specified in stream properties" }
        assert(config.properties.containsKey("global_store_name")) { "Global store name not specified in stream properties" }

        defineStream(
            StreamDefinition(
                inputTopic = KafkaTopic(config.properties["input_topic"] as String),
                outputTopic = KafkaTopic( config.properties["output_topic"] as String),
                globalStoreConfig = GlobalStoreConfig(
                    storeName = config.properties["global_store_name"] as String,
                    topic = KafkaTopic(config.properties["global_store_topic"] as String),
                ),
            )
        )
    }

}

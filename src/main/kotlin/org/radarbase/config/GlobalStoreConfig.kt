package org.radarbase.config

import org.radarbase.topic.KafkaTopic

data class GlobalStoreConfig(
    val storeName: String,
    val topic: KafkaTopic,
)

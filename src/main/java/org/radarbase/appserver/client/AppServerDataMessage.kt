package org.radarbase.appserver.client

data class AppServerDataMessage(
    val ttlSeconds: Long,
    val sourceId: String? = null,
    val sourceType: String = "aRMT",
    val appPackage: String = "org.phidatalab.radar_armt",
    val scheduledTime: String,
    val dataMap: Map<String, String> = mapOf(),
) : AppServerMessageContents

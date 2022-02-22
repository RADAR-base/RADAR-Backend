package org.radarbase.appserver.client

data class AppServerNotification(
    val title: String,
    val body: String,
    val ttlSeconds: Long,
    val sourceId: String? = null,
    val type: String,
    val sourceType: String = "aRMT",
    val appPackage: String = "org.phidatalab.radar_armt",
    val scheduledTime: String,
    val additionalData: Map<String, String> = mapOf(),
) : AppServerMessageContents

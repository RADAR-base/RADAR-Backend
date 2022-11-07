package org.radarbase.appserver.client

data class AppServerData(
    val ttlSeconds: Long,
    val sourceId: String,
    val sourceType: String = "aRMT",
    val appPackage: String = "org.phidatalab.radar_armt",
    val scheduledTime: String,
    val dataMap: String,
) : AppServerMessageContents

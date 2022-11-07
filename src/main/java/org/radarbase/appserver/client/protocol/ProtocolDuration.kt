package org.radarbase.appserver.client.protocol

data class ProtocolDuration(
    val amount: Long,
    val unit: String = "min",
)

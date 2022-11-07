package org.radarbase.appserver.client.protocol

data class Reminders(
    val repeat: Int = 0,
    val amount: Int = 0,
    val unit: String = "day",
)

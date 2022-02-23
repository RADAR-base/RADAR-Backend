package org.radarbase.appserver.client.protocol

data class RepeatQuestionnaire(
    val unitsFromZero: List<Long> = listOf(),
    val unit: String = "min",
)

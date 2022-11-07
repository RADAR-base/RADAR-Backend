package org.radarbase.appserver.client.protocol

data class RepeatQuestionnaire(
    val unitsFromZero: List<Int> = listOf(),
    val unit: String = "min",
)

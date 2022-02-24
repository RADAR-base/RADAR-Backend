package org.radarbase.appserver.client.protocol

data class Notification(
    val title: MultiLingualText = mapOf(
        "en" to defaultNotificationTitle,
    ),
    val text: MultiLingualText = mapOf(
        "en" to defaultNotificationText,
    ),
) {
    companion object {
        const val defaultNotificationTitle = "Questionnaire Time"
        const val defaultNotificationText = "Urgent Questionnaire Pending. Please complete now."
    }
}

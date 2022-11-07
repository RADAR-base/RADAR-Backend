package org.radarcns.consumer.realtime.action.appserver

/**
 * Provides data and notification message content to create a message in the Appserver for scheduled
 * delivery through FCM.
 */
interface NotificationContentProvider {
    val dataMessage: String
    val notificationMessage: String
}
package org.radarbase.appserver.client

enum class MessagingType(val urlPart: String) {
    NOTIFICATIONS("notifications"), DATA("data"), ALL("all");
}

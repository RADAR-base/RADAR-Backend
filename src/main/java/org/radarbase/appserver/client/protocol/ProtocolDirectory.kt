package org.radarbase.appserver.client.protocol

interface ProtocolDirectory {
    fun get(
        projectId: String,
        userId: String,
        attributes: Map<String, String>
    ): QuestionnaireTrigger?
}

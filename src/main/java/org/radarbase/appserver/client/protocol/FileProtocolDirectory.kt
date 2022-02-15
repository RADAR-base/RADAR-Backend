package org.radarbase.appserver.client.protocol

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.stream.Collectors

class FileProtocolDirectory(
    protocolDir: String,
    mapper: ObjectMapper,
): ProtocolDirectory {
    private val protocols: Map<String, QuestionnaireTrigger>

    init {
        val jsonReader = mapper.readerFor(QuestionnaireTrigger::class.java)

        protocols = Files.list(Paths.get(protocolDir))
            .filter { it.endsWith(".json") }
            .map { path ->
                path to try {
                    Files.newInputStream(path).use {
                        jsonReader.readValue<QuestionnaireTrigger>(it)
                    }
                } catch (ex: Exception) {
                    logger.error("Failed to read JSON protocol {}", path, ex)
                    null
                }
            }
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(
                { (path, _) ->
                    path.fileName.toString()
                        .lowercase()
                        .removeSuffix(".json")
                },
                { (_, value) -> value as QuestionnaireTrigger },
            ))
    }

    override fun get(
        projectId: String,
        userId: String,
        attributes: Map<String, String>
    ): QuestionnaireTrigger? = protocols[attributes["intervention"] ?: "default"]

    companion object {
        private val logger = LoggerFactory.getLogger(FileProtocolDirectory::class.java)
    }
}

package org.radarbase.appserver.client.protocol

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.stream.Collectors
import kotlin.collections.HashMap
import kotlin.streams.asSequence

class FileProtocolDirectory(
    protocolDir: String,
    mapper: ObjectMapper,
): ProtocolDirectory {
    private val protocols: Map<String, QuestionnaireTrigger>

    init {
        val jsonReader = mapper.readerFor(QuestionnaireTrigger::class.java)

        protocols = Files.walk(Paths.get(protocolDir)).use { pathStream ->
            pathStream
                .filter { path -> path.fileName.toString().endsWith(".json") }
                .asSequence()
                .mapNotNull { path ->
                    try {
                        Pair(
                            path.fileName.toString()
                                .lowercase()
                                .removeSuffix(".json"),
                            Files.newInputStream(path).use {
                                jsonReader.readValue<QuestionnaireTrigger>(it)
                            },
                        )
                    } catch (ex: Exception) {
                        logger.error("Failed to read JSON protocol {}", path, ex)
                        null
                    }
                }
                .toMap(TreeMap())
        }
        logger.info("From {} loaded protocols {}", protocolDir, protocols)
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

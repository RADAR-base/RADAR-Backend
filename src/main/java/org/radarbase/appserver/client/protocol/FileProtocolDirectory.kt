package org.radarbase.appserver.client.protocol

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import java.lang.String.CASE_INSENSITIVE_ORDER
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.streams.asSequence

class FileProtocolDirectory(
    protocolDir: String,
    mapper: ObjectMapper,
): ProtocolDirectory {
    private val protocols: Map<String, QuestionnaireTrigger>

    init {
        val jsonReader = mapper.readerFor(QuestionnaireTrigger::class.java)

        val jsonFileMatcher = FileSystems.getDefault().getPathMatcher("glob:**.json");

        protocols = Files.walk(Paths.get(protocolDir)).use { pathStream ->
            pathStream
                .filter(jsonFileMatcher::matches)
                .asSequence()
                .mapNotNull { path ->
                    try {
                        Pair(
                            path.fileName.toString().removeSuffix(".json"),
                            Files.newInputStream(path).use {
                                jsonReader.readValue<QuestionnaireTrigger>(it)
                            },
                        )
                    } catch (ex: Exception) {
                        logger.error("Failed to read JSON protocol {}", path, ex)
                        null
                    }
                }
                .toMap(TreeMap(CASE_INSENSITIVE_ORDER))
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

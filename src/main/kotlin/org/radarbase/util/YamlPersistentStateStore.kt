package org.radarbase.util

import org.radarbase.config.YamlConfigLoader
import org.radarcns.kafka.ObservationKey
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import kotlin.text.iterator

/**
 * Store a state for a Kafka consumer. This uses a file storage, storing files to YAML format. It
 * uses Jackson for serialization and deserialization, so state objects must be serializable and
 * deserializable with this mechanism.
 */
class YamlPersistentStateStore(private val basePath: File) : PersistentStateStore {
    private val loader = YamlConfigLoader()

    init {
        checkBasePath(basePath)
    }

    override fun <T : Any> retrieveState(groupId: String, clientId: String, stateDefault: T): T {
        val consumerFile = getFile(groupId, clientId)
        if (!consumerFile.exists()) {
            return stateDefault
        }
        @Suppress("UNCHECKED_CAST")
        val stateClass = stateDefault.javaClass as Class<out T>
        return loader.load(consumerFile.toPath(), stateClass)
    }

    override fun storeState(groupId: String, clientId: String, value: Any) {
        loader.store(getFile(groupId, clientId).toPath(), value)
    }

    /** File for given consumer. */
    private fun getFile(groupId: String, clientId: String): File {
        return File(basePath, "${groupId}_$clientId.yml")
    }

    override fun keyToString(key: ObservationKey): String {
        val projectId = key.projectId
        val userId = key.userId
        val sourceId = key.sourceId
        val builder = StringBuilder(
            (projectId?.length ?: 0) + userId.length + 6 + sourceId.length
        )
        projectId?.let { escape(it, builder) }
        builder.append(SEPARATOR)
        escape(userId, builder)
        builder.append(SEPARATOR)
        escape(sourceId, builder)
        return builder.toString()
    }

    private fun escape(string: String, builder: StringBuilder) {
        for (c in string) {
            when (c) {
                '\\' -> builder.append("\\\\")
                SEPARATOR -> builder.append('\\').append(SEPARATOR)
                else -> builder.append(c)
            }
        }
    }

    override fun stringToKey(string: String): ObservationKey {
        val builder = StringBuilder(string.length)
        val key = ObservationKey()
        var hasSlash = false
        var numFound = 0
        for (c in string) {
            when (c) {
                '\\' -> {
                    if (hasSlash) {
                        builder.append(c)
                        hasSlash = false
                    } else {
                        hasSlash = true
                    }
                }
                SEPARATOR -> {
                    if (hasSlash) {
                        builder.append(c)
                        hasSlash = false
                    } else {
                        if (numFound == 0) {
                            numFound++
                            if (builder.isEmpty()) {
                                key.projectId = null
                            } else {
                                key.projectId = builder.toString()
                                builder.setLength(0)
                            }
                        } else {
                            key.userId = builder.toString()
                            builder.setLength(0)
                        }
                    }
                }
                else -> {
                    builder.append(c)
                }
            }
        }
        key.sourceId = builder.toString()
        return key
    }

    companion object {
        private const val SEPARATOR = '#'

        /**
         * Check whether the base path can be made into a valid directory and is writable.
         *
         * @param basePath base path for the persistence store.
         * @throws IOException if the base path is not writable for states.
         */
        @Throws(IOException::class)
        private fun checkBasePath(basePath: File) {
            if (basePath.exists()) {
                if (!basePath.isDirectory) {
                    throw IOException("State path ${basePath.absolutePath} is not a directory")
                }
            } else if (!basePath.mkdirs()) {
                throw IOException("Failed to set up persistent state store for the Kafka Monitor.")
            }

            val testFile = File(basePath, ".check_base_path")
            try {
                FileOutputStream(testFile).use { fout ->
                    fout.write(1)
                }
            } catch (ex: IOException) {
                throw IOException("Cannot write files in directory $basePath", ex)
            }
            testFile.delete()
        }
    }
}

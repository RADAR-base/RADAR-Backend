package org.radarbase.config

import java.util.*

class ConfigUtils {

    companion object {
        fun Map<String, String>.withEnvVars(prefix: String): Map<String, String> =
            this + System.getenv().entries.filter { it.key.startsWith(prefix) }.associate {
                    it.key.substring(prefix.length).lowercase(Locale.US).replace("_", ".") to it.value
                }
    }
}
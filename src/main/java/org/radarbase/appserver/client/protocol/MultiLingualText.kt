package org.radarbase.appserver.client.protocol

typealias MultiLingualText = Map<String, String>

/**
 * Get the translation in the given language.
 * @return non-blank translation or null if not available.
 */
fun MultiLingualText.translation(language: String): String? =
    this[language]?.takeIf { it.isNotBlank() }

/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.kotlin.config

import org.radarbase.RadarBackend
import org.radarbase.config.YamlConfigLoader
import org.radarbase.util.PersistentStateStore
import org.radarbase.util.YamlPersistentStateStore
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.net.URISyntaxException
import java.util.*

/**
 * Java Singleton class for handling the yml config file. Implements [RadarPropertyHandler]
 */
class RadarPropertyHandlerImpl : RadarPropertyHandler {
    private var _properties: ConfigRadar? = null
    private var _kafkaProperty: KafkaProperty? = null

    override val radarProperties: ConfigRadar
        get() = _properties ?: throw IllegalStateException("Properties cannot be accessed without calling load() first")

    override fun isLoaded(): Boolean = _properties != null

    @Throws(IOException::class)
    override fun load(pathFile: String?) {
        if (isLoaded()) {
            throw IllegalStateException("Properties class has been already loaded")
        }

        val file = if (pathFile.isNullOrEmpty()) {
            getDefaultFile().also {
                log.info("DEFAULT CONFIGURATION: loading config file at {}", it)
            }
        } else {
            log.info("USER CONFIGURATION: loading config file at {}", pathFile)
            File(pathFile)
        }

        require(file.exists()) { "Config file $file does not exist" }
        require(file.isFile) { "Config file $file is invalid" }

        _properties = YamlConfigLoader().load(file.toPath(), ConfigRadar::class.java)

        val buildProperties = Properties()
        javaClass.getResourceAsStream("/build.properties")?.use {
            buildProperties.load(it)
        }
        buildProperties.getProperty("version")?.let {
            _properties?.buildVersion = it
        }
    }

    @Throws(IOException::class)
    private fun getDefaultFile(): File {
        var localFile = File(CONFIG_FILE_NAME)
        if (!localFile.exists()) {
            try {
                val codePathUrl = RadarBackend::class.java.protectionDomain.codeSource.location
                val codePath = codePathUrl.toURI().path
                val codeDir = codePath.substring(0, codePath.lastIndexOf('/') + 1)
                localFile = File(codeDir, CONFIG_FILE_NAME)
            } catch (ex: URISyntaxException) {
                throw IOException("Cannot get path of executable", ex)
            }
        }
        return localFile
    }

    override val kafkaProperties: KafkaProperty
        get() = _kafkaProperty ?: KafkaProperty(radarProperties).also { _kafkaProperty = it }

    @Throws(IOException::class)
    override fun getPersistentStateStore(): PersistentStateStore? {
        return radarProperties.persistencePath?.let {
            YamlPersistentStateStore(File(it))
        }
    }

    companion object {
        private const val CONFIG_FILE_NAME = "radar.yml"
        private val log = LoggerFactory.getLogger(RadarPropertyHandlerImpl::class.java)
    }
}

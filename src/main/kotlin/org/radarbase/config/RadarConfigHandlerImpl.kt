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

package org.radarbase.config

import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.radarbase.RadarBackend
import org.radarbase.util.PersistentStateStore
import org.radarbase.util.YamlPersistentStateStore
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URISyntaxException
import java.nio.file.Path
import java.util.*
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile

/**
 * Singleton class for handling the yml config file. Implements [RadarConfigHandler]
 */
class RadarConfigHandlerImpl : RadarConfigHandler {
    private var _properties: RadarBackendConfig? = null
    private var _kafkaProperty: KafkaProperty? = null

    override val radarProperties: RadarBackendConfig
        get() = _properties ?: throw IllegalStateException("Properties cannot be accessed without calling load() first")

    override fun isLoaded(): Boolean = _properties != null

    @Throws(IOException::class)
    override fun load(pathFile: String?) {
        assert(!isLoaded()) { "Properties class has been already loaded" }

        val file = if (pathFile.isNullOrEmpty()) {
            getDefaultFile().also {
                log.info("DEFAULT CONFIGURATION: loading config file at {}", it)
            }
        } else {
            Path.of(pathFile).also {
                log.info("USER CONFIGURATION: loading config file at {}",  it)
            }
        }

        require(file.exists()) { "Config file $file does not exist" }
        require(file.isRegularFile()) { "Config file $file is invalid" }

        _properties = YamlConfigLoader { mapper -> mapper.registerKotlinModule() }
            .load(file, RadarBackendConfig::class.java)

        val buildProperties = Properties()
        javaClass.getResourceAsStream("/build.properties")?.use {
            buildProperties.load(it)
        }
        buildProperties.getProperty("version")?.let {
            _properties?.buildVersion = it
        }
    }

    @Throws(IOException::class)
    private fun getDefaultFile(): Path {
        var localFile = Path.of(CONFIG_FILE_NAME)
        if (!localFile.exists()) {
            try {
                val codePathUrl = RadarBackend::class.java.protectionDomain.codeSource.location
                val codePath = codePathUrl.toURI().path
                val codeDir = codePath.take(codePath.lastIndexOf('/') + 1)
                localFile = Path.of(codeDir, CONFIG_FILE_NAME)
            } catch (ex: URISyntaxException) {
                throw IOException("Cannot get path of executable", ex)
            }
        }
        return localFile
    }

    override val kafkaProperties: KafkaProperty
        get() = _kafkaProperty ?: KafkaProperty(radarProperties).also { _kafkaProperty = it }

    @Throws(IOException::class)
    override fun getPersistentStateStore(): PersistentStateStore? =
        radarProperties.persistencePath?.let {
            YamlPersistentStateStore(Path.of(it))
        }

    companion object {
        private val log = LoggerFactory.getLogger(RadarConfigHandlerImpl::class.java)
        private const val CONFIG_FILE_NAME = "radar.yml"
    }
}

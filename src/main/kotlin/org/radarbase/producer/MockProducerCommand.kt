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

package org.radarbase.producer

import org.radarbase.config.BackendProcess
import org.radarbase.config.MockConfig
import org.radarbase.config.RadarBackendCliOptions
import org.radarbase.config.RadarConfigHandler
import org.radarbase.config.YamlConfigLoader
import org.radarbase.mock.MockProducer
import org.radarbase.mock.config.BasicMockConfig
import org.slf4j.LoggerFactory
import java.io.IOException

class MockProducerCommand(
    cliOptions: RadarBackendCliOptions,
    radarConfigHandler: RadarConfigHandler,
) : BackendProcess {
    private val producer: MockProducer

    init {
        val radar = radarConfigHandler.radarProperties
        val producerConfig = BasicMockConfig()
        val mockFile = cliOptions.mockFile

        if (mockFile != null) {
            val mockConfig = YamlConfigLoader().load(mockFile, MockConfig::class.java)
            producerConfig.data = mockConfig.data
        } else {
            producerConfig.numberOfDevices = cliOptions.numMockDevices
        }

        producerConfig.restProxy = radar.restProxy
        producerConfig.schemaRegistry = radar.schemaRegistry?.get(0)
        producerConfig.producerMode = if (cliOptions.isMockDirect) "direct" else "rest"
        producer = MockProducer(producerConfig)
    }

    @Throws(IOException::class)
    override fun start() {
        // producer.start()
    }

    @Throws(IOException::class, InterruptedException::class)
    override fun shutdown() {
        // producer.shutdown()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MockProducerCommand::class.java)
    }
}

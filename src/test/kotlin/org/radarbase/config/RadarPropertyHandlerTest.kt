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

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RadarPropertyHandlerTest {

    private lateinit var propertyHandler: RadarPropertyHandler

    @BeforeEach
    fun setUp() {
        this.propertyHandler = RadarPropertyHandlerImpl()
    }

    @Test
    fun getInstanceEmptyProperties() {
        assertFailsWith(IllegalStateException::class) {
            propertyHandler.radarProperties
        }
    }

    @Test
    fun loadWithInvalidFilePath() {
        val invalidPath = "/usr/"
        assertFailsWith(IllegalArgumentException::class) {
            propertyHandler.load(invalidPath)
        }
    }

    @Test
    @Throws(Exception::class)
    fun load() {
        propertyHandler.load("src/test/resources/config/radar.yml")

        val properties = propertyHandler.radarProperties
        assertNotNull(properties.broker)
        assertNotNull(properties.brokerPaths)
        assertNotNull(properties.released)
        assertNotNull(properties.schemaRegistry)
        assertNotNull(properties.schemaRegistryPaths)
        assertNotNull(properties.zookeeper)
        assertNotNull(properties.zookeeperPaths)
        assertNotNull(properties.version)
        assertNotNull(properties.extras)
        assert(properties.extras!!.containsKey("somethingother"))
        assertEquals("bla", properties.extras!!["somethingother"])
    }

    @Test
    @Throws(Exception::class)
    fun loadInvalidYaml() {
        assertFailsWith(UnrecognizedPropertyException::class) {
            propertyHandler.load("src/test/resources/config/invalidradar.yml")
        }
    }

    @Test
    fun loadInvalidStreamPriority() {
        assertFailsWith(JsonMappingException::class) {
            propertyHandler.load("src/test/resources/config/invalid_stream_priority.yml")
        }
    }

    @Test
    @Throws(Exception::class)
    fun loadWithInstance() {
        assertFailsWith(IllegalStateException::class) {
            propertyHandler.load("radar.yml")
            propertyHandler.load("again.yml")
        }
    }

    @Test
    fun getKafkaPropertiesBeforeLoad() {
        assertFailsWith(IllegalStateException::class) {
            propertyHandler.kafkaProperties
        }
    }

    @Test
    @Throws(Exception::class)
    fun getKafkaProperties() {
        propertyHandler.load("radar.yml")
        val property = propertyHandler.kafkaProperties
        assertNotNull(property)
    }
}

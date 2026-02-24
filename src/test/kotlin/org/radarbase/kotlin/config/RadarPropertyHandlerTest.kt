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

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.hasEntry
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Before
import org.junit.Test
import org.radarbase.kotlin.config.RadarPropertyHandler
import org.radarbase.kotlin.config.RadarPropertyHandlerImpl
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException

class RadarPropertyHandlerTest {

    private lateinit var propertyHandler: RadarPropertyHandler

    @Before
    fun setUp() {
        this.propertyHandler = RadarPropertyHandlerImpl()
    }

    @Test(expected = IllegalStateException::class)
    fun getInstanceEmptyProperties() {
        propertyHandler.radarProperties
    }

    @Test(expected = IllegalArgumentException::class)
    @Throws(Exception::class)
    fun loadWithInvalidFilePath() {
        val invalidPath = "/usr/"
        propertyHandler.load(invalidPath)
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
        assertThat(properties.extras, hasEntry("somethingother", "bla"))
    }

    @Test(expected = UnrecognizedPropertyException::class)
    @Throws(Exception::class)
    fun loadInvalidYaml() {
        propertyHandler.load("src/test/resources/config/invalidradar.yml")
    }

    @Test(expected = JsonMappingException::class)
    @Throws(Exception::class)
    fun loadInvalidStreamPriority() {
        propertyHandler.load("src/test/resources/config/invalid_stream_priority.yml")
    }

    @Test(expected = IllegalStateException::class)
    @Throws(Exception::class)
    fun loadWithInstance() {
        propertyHandler.load("radar.yml")
        propertyHandler.load("again.yml")
    }

    @Test(expected = IllegalStateException::class)
    fun getKafkaPropertiesBeforeLoad() {
        propertyHandler.kafkaProperties
    }

    @Test
    @Throws(Exception::class)
    fun getKafkaProperties() {
        propertyHandler.load("radar.yml")
        val property = propertyHandler.kafkaProperties
        assertNotNull(property)
    }
}

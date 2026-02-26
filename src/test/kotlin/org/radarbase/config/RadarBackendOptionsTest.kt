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

import org.apache.commons.cli.ParseException
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class RadarBackendOptionsTest {
    @Test
    @Throws(ParseException::class)
    fun empty() {
        val opts = RadarBackendOptions.parse(arrayOf())
        assertNull(opts.subCommand)
        assertNull(opts.subCommandArgs)
        assertNull(opts.propertyPath)
    }

    @Test
    @Throws(ParseException::class)
    fun withConfig() {
        val opts = RadarBackendOptions.parse(arrayOf("-c", "cfg"))
        assertNull(opts.subCommand)
        assertNull(opts.subCommandArgs)
        assertEquals("cfg", opts.propertyPath)
    }

    @Test
    @Throws(ParseException::class)
    fun withSubcommand() {
        val opts = RadarBackendOptions.parse(arrayOf("-c", "cfg", "stream"))
        assertEquals("stream", opts.subCommand)
        assertArrayEquals(arrayOf<String>(), opts.subCommandArgs)
        assertEquals("cfg", opts.propertyPath)
    }

    @Test
    @Throws(ParseException::class)
    fun withSubcommandArgs() {
        val opts = RadarBackendOptions.parse(arrayOf("monitor", "battery"))
        assertEquals("monitor", opts.subCommand)
        assertArrayEquals(arrayOf("battery"), opts.subCommandArgs)
    }
}

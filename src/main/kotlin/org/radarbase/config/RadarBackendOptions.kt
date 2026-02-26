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

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.slf4j.LoggerFactory
import java.io.File

class RadarBackendOptions(private val cli: CommandLine) {
    val subCommand: String?
    val subCommandArgs: Array<String>?

    init {
        log.info("Loading configuration")
        val additionalArgs = cli.args

        if (additionalArgs.isNotEmpty()) {
            subCommand = additionalArgs[0]
            subCommandArgs = additionalArgs.sliceArray(1 until additionalArgs.size)
        } else {
            subCommand = null
            subCommandArgs = null
        }
    }

    val propertyPath: String?
        get() = cli.getOptionValue("config")

    val numMockDevices: Int
        get() = cli.getOptionValue("devices", "1").toInt()

    val isMockDirect: Boolean
        get() = cli.hasOption("direct")

    val mockFile: File?
        get() = cli.getOptionValue("file")?.let { File(it) }

    companion object {
        private val log = LoggerFactory.getLogger(RadarBackendOptions::class.java)
        val OPTIONS: Options = Options().addOption("c", "config", true, "Configuration YAML file")
            .addOption("d", "devices", true, "Number of devices to use with the mock command.").addOption(
                "D",
                "direct",
                false,
                "The mock device will bypass the rest-proxy and use the Kafka Producer API instead.",
            ).addOption("f", "file", true, "Read mock data from given configuration file.")

        @Throws(ParseException::class)
        fun parse(args: Array<String>): RadarBackendOptions {
            val cli = DefaultParser().parse(OPTIONS, args)
            return RadarBackendOptions(cli)
        }
    }
}

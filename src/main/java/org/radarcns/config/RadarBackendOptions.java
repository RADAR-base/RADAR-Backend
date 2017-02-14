/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.config;

import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class RadarBackendOptions {
    private static final Logger log = LoggerFactory.getLogger(RadarBackendOptions.class);
    private final CommandLine cli;
    private final String subCommand;
    private final String[] subCommandArgs;
    public static final Options OPTIONS = new Options()
            .addOption("c", "config", true, "Configuration YAML file")
            .addOption("d", "devices", true, "Number of devices to use with the mock command.")
            .addOption("D", "direct", false, "The mock device will bypass the rest-proxy and use "
                    + "the Kafka Producer API instead.")
            .addOption("f", "file", true, "Read mock data from given configuration file.");


    /**
     * @param cli command line arguments given
     */
    public RadarBackendOptions(CommandLine cli) {
        log.info("Loading configuration");
        this.cli = cli;

        String[] additionalArgs = this.cli.getArgs();

        if (additionalArgs.length > 0) {
            subCommand = additionalArgs[0];
            subCommandArgs = new String[additionalArgs.length - 1];
            System.arraycopy(additionalArgs, 1, subCommandArgs, 0, subCommandArgs.length);
        } else {
            subCommand = null;
            subCommandArgs = null;
        }
    }

    public static RadarBackendOptions parse(@Nonnull String[] args) throws ParseException {
        CommandLine cli = new PosixParser().parse(OPTIONS, args);
        return new RadarBackendOptions(cli);
    }

    public String getPropertyPath() {
        return this.cli.getOptionValue("config", null);
    }

    public int getNumMockDevices() {
        return Integer.parseInt(this.cli.getOptionValue("devices", "1"));
    }

    public boolean isMockDirect() {
        return this.cli.hasOption("direct");
    }

    public File getMockFile() {
        String file = this.cli.getOptionValue("file", null);
        if (file == null) {
            return null;
        } else {
            return new File(file);
        }
    }

    public String getSubCommand() {
        return subCommand;
    }

    public String[] getSubCommandArgs() {
        return subCommandArgs;
    }
}

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

package org.radarcns.config;

import java.io.File;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RadarBackendOptions {
    private static final Logger log = LoggerFactory.getLogger(RadarBackendOptions.class);
    private final CommandLine cli;
    private final String subCommand;
    private final List<String> subCommandArgs;
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

        List<String> additionalArgs = this.cli.getArgList();

        if (additionalArgs.isEmpty()) {
            subCommand = null;
            subCommandArgs = List.of();
        } else {
            subCommand = additionalArgs.get(0);
            subCommandArgs = Collections.unmodifiableList(
                    additionalArgs.subList(1, additionalArgs.size()));
        }
    }

    public static RadarBackendOptions parse(@Nonnull String... args) throws ParseException {
        CommandLine cli = new DefaultParser().parse(OPTIONS, args);
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

    @Nonnull
    public List<String> getSubCommandArgs() {
        return subCommandArgs;
    }
}

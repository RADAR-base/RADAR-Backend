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

package org.radarcns;

import org.apache.commons.cli.ParseException;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SubCommand;
import org.radarcns.monitor.KafkaMonitorFactory;
import org.radarcns.producer.MockProducerCommand;
import org.radarcns.stream.KafkaStreamFactory;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;

/**
 * Core class that initialises configurations and then start all needed Kafka streams
 */
public final class RadarBackend {
    private static final Logger log = LoggerFactory.getLogger(RadarBackend.class);
    private final RadarBackendOptions options;
    private final RadarPropertyHandler radarPropertyHandler;
    private SubCommand command;

    public RadarBackend(@Nonnull RadarBackendOptions options) throws IOException {
        this(options, createPropertyHandler(options));
    }

    public RadarBackend(@Nonnull RadarBackendOptions options,
            @Nonnull RadarPropertyHandler properties) {
        this.options = options;
        this.radarPropertyHandler = properties;

        log.info("Configuration successfully updated");
        log.info("radar.yml configuration: {}", radarPropertyHandler.getRadarProperties());
    }

    private static RadarPropertyHandler createPropertyHandler(@Nonnull RadarBackendOptions options)
            throws IOException {
        RadarPropertyHandler properties = RadarSingletonFactory.getRadarPropertyHandler();
        properties.load(options.getPropertyPath());
        return properties;
    }

    public SubCommand createCommand() throws IOException {
        String subCommand = options.getSubCommand();
        if (subCommand == null) {
            subCommand = "stream";
        }
        switch (subCommand) {
            case "stream":
                return new KafkaStreamFactory(options, radarPropertyHandler).createStreamMaster();
            case "monitor":
                return new KafkaMonitorFactory(options, radarPropertyHandler).createMonitor();
            case "mock":
                return new MockProducerCommand(options, radarPropertyHandler);
            default:
                throw new IllegalArgumentException("Unknown subcommand "
                        + options.getSubCommand());
        }
    }

    /**
     * It starts streams and sets a ShutdownHook to close streams while closing the application
     */
    public void application() {
        try {
            start();
        } catch (IOException ex) {
            log.error("FATAL ERROR! The current instance cannot start", ex);
            System.exit(1);
        } catch (InterruptedException ex) {
            log.error("The current instance was interrupted", ex);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdown();
            } catch (Exception ex) {
                log.error("Impossible to finalise the shutdown hook", ex);
            }
        }));
    }

    /**
     * Start here all needed StreamMaster
     *
     * @throws IOException if the command failed to start up
     * @throws InterruptedException if the command was interrupted
     */
    public void start() throws IOException, InterruptedException {
        log.info("STARTING");

        command = createCommand();
        command.start();

        log.info("STARTED");
    }

    /**
     * Stop here all commands started inside {@link #start()}.
     *
     * @throws IOException if the command failed to shut down
     * @throws InterruptedException if the command was interrupted before completely shutting down
     */
    public void shutdown() throws InterruptedException, IOException {
        log.info("SHUTTING DOWN");

        command.shutdown();

        log.info("FINISHED");
    }

    public static void main(String[] args) {
        try {
            RadarBackendOptions options = RadarBackendOptions.parse(args);
            RadarBackend backend = new RadarBackend(options);
            backend.application();
        } catch (ParseException ex) {
            log.error("Cannot parse arguments {}. Valid options are:\n{}",
                    Arrays.toString(args), RadarBackendOptions.OPTIONS);
            System.exit(1);
        } catch (Exception ex) {
            log.error("Failed to run command", ex);
            System.exit(1);
        }
    }
}

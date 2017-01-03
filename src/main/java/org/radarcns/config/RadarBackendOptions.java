package org.radarcns.config;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class RadarBackendOptions {
    private static final Logger log = LoggerFactory.getLogger(RadarBackendOptions.class);
    private final RadarPropertyHandler radarPropertyHandler;
    private final CommandLine cli;
    private final String subCommand;
    private final String[] subCommandArgs;
    private String monitorName;
    public static Options OPTIONS = new Options()
        .addOption("c", "config", true, "Configuration YAML file");


    /**
     * @param cli: command line arguments given
     */
    public RadarBackendOptions(CommandLine cli) {
        log.info("Loading configuration");
        radarPropertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
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

        try {
            radarPropertyHandler.load(getPropertyPath());
        } catch (Exception e) {
            log.error("FATAL ERROR: application is shutting down", e);
            System.exit(1);
        }

        log.info("Configuration successfully updated");
        log.info(radarPropertyHandler.getRadarProperties().info());
    }

    public static RadarBackendOptions parse(@Nonnull String[] args) throws ParseException {
        CommandLine cli = new PosixParser().parse(OPTIONS, args);
        return new RadarBackendOptions(cli);
    }

    public String getPropertyPath() {
        return this.cli.getOptionValue("config", null);
    }

    public RadarPropertyHandler getRadarPropertyHandler() {
        return radarPropertyHandler;
    }

    public String getSubCommand() {
        return subCommand;
    }

    public String[] getSubCommandArgs() {
        return subCommandArgs;
    }

    public Options getOptions() {
        return OPTIONS;
    }
}

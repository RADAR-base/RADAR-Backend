package org.radarcns;

import org.apache.commons.cli.ParseException;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SubCommand;
import org.radarcns.empatica.E4Worker;
import org.radarcns.process.KafkaMonitorFactory;
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
        this.options = options;

        radarPropertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        radarPropertyHandler.load(options.getPropertyPath());

        log.info("Configuration successfully updated");
        log.info("radar.yml configuration: {}", radarPropertyHandler.getRadarProperties());
    }

    public SubCommand createCommand() throws IOException {
        String subCommand = options.getSubCommand();
        ConfigRadar properties = radarPropertyHandler.getRadarProperties();
        if (subCommand == null) {
            subCommand = "stream";
        }
        switch (subCommand) {
            case "stream":
                return new E4Worker(properties.isStandalone());
            case "monitor":
                return new KafkaMonitorFactory(options, properties).createMonitor();
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
            go();
        } catch (IOException ex) {
            log.error("FATAL ERROR! The current instance cannot start", ex);
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try {
                finish();
            } catch (Exception ex) {
                log.error("Impossible to finalise the shutdown hook", ex);
            }
        }));
    }

    /**
     * Start here all needed MasterAggregator
     * @see org.radarcns.stream.aggregator.MasterAggregator
     */
    private void go() throws IOException{
        log.info("STARTING");

        command = createCommand();
        command.start();

        log.info("STARTED");
    }

    /**
     * Stop here all MasterAggregators started inside the @link org.radarcns.RadarBackend#run
     *
     * @see org.radarcns.stream.aggregator.MasterAggregator
     */
    private void finish() throws InterruptedException, IOException {
        log.info("SHUTTING DOWN");

        command.shutdown();

        log.info("FINISHED");
    }

    public static void main(String[] args){
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

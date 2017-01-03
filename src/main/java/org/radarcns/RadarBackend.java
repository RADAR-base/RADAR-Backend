package org.radarcns;

import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.empatica.E4Worker;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * Core class that initialises configurations and then start all needed Kafka streams
 */
public class RadarBackend {

    private static final Logger log = LoggerFactory.getLogger(RadarBackend.class);
    private static RadarPropertyHandler radarPropertyHandler = RadarSingletonFactory
        .getRadarPropertyHandler();

    public RadarBackend(@Nonnull String[] args) {
        config(args);
        application();
    }

    /**
     * @param args: are the array of Strings given in input to the jar file
     */
    private static void config(@Nonnull String[] args) {
        log.info("Loading configuration");
        try {
            radarPropertyHandler.load(args.length == 0 ? null : args[0]);
        } catch (Exception ex) {
            log.error("FATAL ERROR: application is shutting down", ex);
            System.exit(1);
        }

        log.info("Configuration successfully updated");
        log.info(radarPropertyHandler.getRadarProperties().info());
    }

    /**
     * It starts streams and sets a ShutdownHook to close streams while closing the application
     */
    private static void application() {
        try {
            go();
        } catch (IOException exp) {
            log.error("FATAL ERROR! The current instance cannot start", exp);
            System.exit(0);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                finish();
            } catch (Exception e) {
                log.error("Impossible to finalise the shutdown hook", e);
            }
        }));
    }

    /**
     * Start here all needed MasterAggregator
     *
     * @see org.radarcns.stream.aggregator.MasterAggregator
     */
    private static void go() throws IOException {
        log.info("STARTING");

        E4Worker.getInstance().start();

        log.info("STARTED");
    }

    /**
     * Stop here all MasterAggregators started inside the @link org.radarcns.RadarBackend#run
     *
     * @see org.radarcns.stream.aggregator.MasterAggregator
     */
    private static void finish() throws InterruptedException, IOException {
        log.info("SHUTTING DOWN");

        E4Worker.getInstance().shutdown();

        log.info("FINISHED");
    }
}

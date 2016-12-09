package org.radarcns;

import org.radarcns.config.PropertiesRadar;
import org.radarcns.empaticaE4.E4Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * Core class that initialises configurations and then start all needed Kafka streams
 */
public class RadarBackend {

    private final static Logger log = LoggerFactory.getLogger(RadarBackend.class);

    public RadarBackend(@Nonnull String[] args){
        config(args);
        application();
    }

    /**
    * @param args: are the array of Strings given in input to the jar file
    */
    private static void config(@Nonnull String[] args){
        System.out.println("Loading configuration");

        try {
            PropertiesRadar.load(args.length == 0 ? null : args[0]);
        }
        catch (Exception e) {
            System.out.println("FATAL ERROR: application is shutting down");
            System.out.println(e.toString());
            System.exit(0);
        }

        System.out.println("Configuration successfully updated");

        log.info(PropertiesRadar.getInstance().info());
    }

    /**
     * It starts streams and sets a ShutdownHook to close streams while closing the application
     */
    private static void application(){
        try {
            go();
        } catch (IOException e) {
            log.error("FATAL ERROR! The current instance cannot start",e);
            System.exit(0);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            try {
                finish();
            } catch (Exception e) {
                log.error("Impossible to finalise the shutdown hook",e);
            }
        }));
    }

    /**
     * Start here all needed MasterAggregator
     * @see org.radarcns.stream.aggregator.MasterAggregator
     */
    private static void go() throws IOException{
        log.info("STARTING");

        E4Worker.getInstance().start();

        log.info("STARTED");
    }

    /**
     * Stop here all MasterAggregators started inside the @link org.radarcns.RadarBackend#run
     * @see org.radarcns.stream.aggregator.MasterAggregator
     */
    private static void finish() throws InterruptedException, IOException {
        log.info("SHUTTING DOWN");

        E4Worker.getInstance().shutdown();

        log.info("FINISHED");
    }
}

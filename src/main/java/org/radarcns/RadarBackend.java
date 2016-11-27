package org.radarcns;

import org.radarcns.config.PropertiesRadar;
import org.radarcns.empaticaE4.E4Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Francesco Nobilia on 24/11/2016.
 */
public class RadarBackend {

    private final static Logger log = LoggerFactory.getLogger(RadarBackend.class);

    public RadarBackend(String[] args){
        config(args);
        application();
    }

    private static void config(String[] args){
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

    private static void go() throws IOException{
        log.info("STARTING");

        E4Worker.getInstance().start();

        log.info("STARTED");
    }

    private static void finish() throws InterruptedException, IOException {
        log.info("SHUTTING DOWN");

        E4Worker.getInstance().shutdown();

        log.info("FINISHED");
    }
}

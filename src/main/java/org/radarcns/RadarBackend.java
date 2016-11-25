package org.radarcns;

import org.radarcns.empaticaE4.E4Worker;
import org.radarcns.util.RadarConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Francesco Nobilia on 24/11/2016.
 */
public class RadarBackend {

    private final static Logger log = LoggerFactory.getLogger(RadarBackend.class);

    private static int numThread;

    public RadarBackend(String[] args){
        config(args);
        application();
    }

    private static void config(String[] args){
        System.out.println("Updating configuration");

        if(args.length == 0){
            System.out.println("DEFAULT CONFIGURATION");

            numThread = 1;
        }
        else if(args.length < 2){
            throw new IllegalArgumentException("Missing parameters \n" +
                    " - input 1: log path \n" +
                    " - input 2: number of concurrent threads for each stream");
        }
        else{
            RadarConfig radarConfig = new RadarConfig();

            try {
                radarConfig.updateLog4jConfiguration(args[0]);
            } catch (Exception e) {
                System.out.println("FATAL ERROR: application is shutting down");
                System.exit(0);
            }

            numThread = Integer.parseInt(args[1]);
        }

        System.out.println("Configuration successfully updated");
    }

    private static void application(){
        try {
            go();
        } catch (IOException e) {
            log.error("FATAL ERROR! The current instance cannot start",e);
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

        if(numThread == 1) {
            E4Worker.getInstance().start();
        }
        else {
            E4Worker.getInstance(numThread).start();
        }

        log.info("STARTED");
    }

    private static void finish() throws InterruptedException, IOException {
        log.info("SHUTTING DOWN");

        E4Worker.getInstance().shutdown();

        log.info("FINISHED");
    }
}

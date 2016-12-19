package org.radarcns.config;

import com.google.common.base.Strings;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.radarcns.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Java Singleton class for handling the yml config file
 */
public class PropertiesRadar {
    private static final Logger log = LoggerFactory.getLogger(PropertiesRadar.class);

    protected ConfigRadar config;

    private static final String nameConfigFile = "radar.yml";
    private static final String nameLogFile = "backend.log";

    private static PropertiesRadar instance;

    public enum Priority {
        LOW("low"), NORMAL("normal"), HIGH("high");

        private final String param;

        Priority(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

    public static ConfigRadar getInstance(){
        if(instance == null){
            throw new IllegalStateException("Property cannot be accessed without calling load() first");
        }

        return instance.config;
    }

    /**
     * @param pathFile: location of configuration file. If null, it tries to check whether there is
     *                  a configuration file in the same location of its jar
     * @throws IllegalStateException if an instance is already available. You cannot load twice a singleton class
     * @throws IllegalArgumentException if either the pathFile does not point to a valid config file
     *                                  or the pathFile is null and there is any valid config file in the same jar location
     */
    public static void load(@Nullable String pathFile) throws Exception {

        String message = "USER CONFIGURATION";

        if (instance != null) {
            throw new IllegalStateException("Property class has been already loaded");
        }

        //If pathFile is null
        if (Strings.isNullOrEmpty(pathFile)) {
            pathFile = Main.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
            pathFile = pathFile.substring(0,pathFile.lastIndexOf('/') + 1)+ nameConfigFile;
            message = "DEFAULT CONFIGURATION";
        }
        log.info("{}: loading config file at {}", message, pathFile);

        File file = new File(pathFile);

        message = null;
        if (!file.isFile()) {
            message = "Config file is invalid";
        }
        if (!file.exists()) {
            message = "Config file does not exist";
        }
        if (message != null) {
            log.error(message);
            throw new IllegalArgumentException(message);
        }

        instance = new PropertiesRadar(pathFile);

        //TODO: add check to validate configuration file. Remember
        //  - log path can be only null, all others have to be stated
        //  - mode can be standalone or high_performance
        //  - all thread priority must be bigger than 1

        if(!Strings.isNullOrEmpty(instance.config.getLog_path())) {
            updateLog4jConfiguration(instance.config.getLog_path());
        }
    }

    /**
     * @param pathFile: location of yml file is going to be loaded
     * @throws IOException if the file does not exist or it does not respect the ConfigRadar java class
     * @see org.radarcns.config.ConfigRadar
     */
    private PropertiesRadar(@Nonnull String pathFile) throws IOException{
        try{
            Yaml yaml = new Yaml();
            InputStream in = Files.newInputStream(Paths.get(pathFile));
            this.config = yaml.loadAs(in, ConfigRadar.class);
        } catch (IOException ex){
            log.error("Impossible load properties", ex);
            throw ex;
        }
    }

    /**
     * @param logPath: new log file defined by the user
     * @throws IllegalArgumentException if logPath is null or is not a valid file
     */
    private static void updateLog4jConfiguration(@Nonnull String logPath) throws Exception {
        String message = null;

        if(Strings.isNullOrEmpty(logPath)){
            message = "Invalid log_path - check your configuration file";
            log.error(message);
            throw new IllegalArgumentException(message);
        }

        if(logPath.lastIndexOf('/') != logPath.length() - 1){
            logPath += "/";
        }

        File file = new File(logPath);
        if(!file.exists()){
            message = "User Log path does not exist";
        }
        else if(!file.isDirectory()){
            message = "User Log path is not a directory";
        }
        if(message != null){
            log.error(message);
            throw new IllegalArgumentException(message);
        }

        logPath += nameLogFile;

        java.util.Properties props = new java.util.Properties();
        try {
            InputStream configStream = instance.getClass().getResourceAsStream( "/log4j.properties");
            props.load(configStream);
            configStream.close();
        } catch (IOException e) {
            log.error("Error during configuration file loading", e);
            throw e;
        }
        props.setProperty("log4j.appender.file.File", logPath);
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);

        log.info("Log path has been correctly configured to {}", logPath);
        log.info("All future messages will be redirected to the log file");
    }
}

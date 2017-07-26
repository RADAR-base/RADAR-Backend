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

import static org.radarcns.util.Strings.isNullOrEmpty;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import javax.annotation.Nonnull;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.radarcns.RadarBackend;
import org.radarcns.util.PersistentStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java Singleton class for handling the yml config file. Implements @link{ RadarPropertyHandler}
 */
public class RadarPropertyHandlerImpl implements RadarPropertyHandler {
    private static final String CONFIG_FILE_NAME = "radar.yml";
    private static final String LOG_FILE_NAME = "backend.log";
    private static final Logger log = LoggerFactory.getLogger(RadarPropertyHandlerImpl.class);

    private ConfigRadar properties;
    private KafkaProperty kafkaProperty;

    @Override
    public ConfigRadar getRadarProperties() {
        if (!isLoaded()) {
            throw new IllegalStateException(
                    "Properties cannot be accessed without calling load() first");
        }
        return properties;
    }

    public boolean isLoaded() {
        return properties != null;
    }

    @Override
    public void load(String pathFile) throws IOException {
        if (isLoaded()) {
            throw new IllegalStateException("Properties class has been already loaded");
        }

        File file;

        //If pathFile is null
        if (isNullOrEmpty(pathFile)) {
            file = getDefaultFile();
            log.info("DEFAULT CONFIGURATION: loading config file at {}", file);
        } else {
            log.info("USER CONFIGURATION: loading config file at {}", pathFile);
            file = new File(pathFile);
        }

        if (!file.exists()) {
            throw new IllegalArgumentException("Config file " + file + " does not exist");
        }
        if (!file.isFile()) {
            throw new IllegalArgumentException("Config file " + file + " is invalid");
        }

        properties = new YamlConfigLoader().load(file, ConfigRadar.class);

        //TODO: add check to validate configuration file. Remember
        //  - log path can be only null, all others have to be stated
        //  - mode can be standalone or high_performance
        //  - all thread priority must be bigger than 1

        if (!isNullOrEmpty(getRadarProperties().getLogPath())) {
            updateLog4jConfiguration(getRadarProperties().getLogPath());
        }
    }

    private File getDefaultFile() throws IOException {
        File localFile = new File(CONFIG_FILE_NAME);
        if (!localFile.exists()) {
            try {
                URL codePathUrl = RadarBackend.class.getProtectionDomain().getCodeSource()
                        .getLocation();
                String codePath = codePathUrl.toURI().getPath();
                String codeDir = codePath.substring(0, codePath.lastIndexOf('/') + 1);
                localFile = new File(codeDir, CONFIG_FILE_NAME);
            } catch (URISyntaxException ex) {
                throw new IOException("Cannot get path of executable", ex);
            }
        }
        return localFile;
    }


    @Override
    public KafkaProperty getKafkaProperties() {
        if (this.kafkaProperty == null) {
            this.kafkaProperty = new KafkaProperty(getRadarProperties());
        }
        return this.kafkaProperty;
    }

    /**
     * @param logPath new log file defined by the user
     * @throws IllegalArgumentException if logPath is null or is not a valid file
     */
    private void updateLog4jConfiguration(@Nonnull String logPath)
            throws IllegalArgumentException, IOException {
        if (isNullOrEmpty(logPath)) {
            throw new IllegalArgumentException("Invalid log_path - check your configuration file");
        }

        File logPathDirectory = new File(logPath);
        if (!logPathDirectory.exists()) {
            throw new IllegalArgumentException("User Log path does not exist");
        } else if (!logPathDirectory.isDirectory()) {
            throw new IllegalArgumentException("User Log path is not a directory");
        }

        java.util.Properties props = new java.util.Properties();
        try (InputStream in = this.getClass().getResourceAsStream("/log4j.properties")) {
            props.load(in);
        } catch (IOException e) {
            log.error("Error during configuration file loading", e);
            throw e;
        }

        File logFile = new File(logPath, LOG_FILE_NAME);

        props.setProperty("log4j.appender.file.File", logFile.getAbsolutePath());
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);

        log.info("Log path has been correctly configured to {}", logFile.getAbsolutePath());
        log.info("All future messages will be redirected to the log file");
    }

    @Override
    public PersistentStateStore getPersistentStateStore() throws IOException {
        if (getRadarProperties().getPersistencePath() != null) {
            File persistenceDir = new File(getRadarProperties().getPersistencePath());
            return new PersistentStateStore(persistenceDir);
        } else {
            return null;
        }
    }
}

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


import static org.radarbase.util.Strings.isNullOrEmpty;

import com.fasterxml.jackson.module.kotlin.KotlinFeature;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.radarbase.config.YamlConfigLoader;
import org.radarcns.RadarBackend;
import org.radarcns.util.PersistentStateStore;
import org.radarcns.util.YamlPersistentStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java Singleton class for handling the yml config file. Implements @link{ RadarPropertyHandler}
 */
public class RadarPropertyHandlerImpl implements RadarPropertyHandler {
    private static final String CONFIG_FILE_NAME = "radar.yml";
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

    @Override
    public boolean isLoaded() {
        return properties != null;
    }

    @Override
    public void load(String pathFile) throws IOException {
        if (isLoaded()) {
            throw new IllegalStateException("Properties class has been already loaded");
        }

        Path file;

        //If pathFile is null
        if (isNullOrEmpty(pathFile)) {
            file = getDefaultFile();
            log.info("DEFAULT CONFIGURATION: loading config file at {}", file);
        } else {
            log.info("USER CONFIGURATION: loading config file at {}", pathFile);
            file = Paths.get(pathFile);
        }

        if (!Files.exists(file)) {
            throw new IllegalArgumentException("Config file " + file + " does not exist");
        }
        properties = new YamlConfigLoader(mapper -> mapper.registerModule(new KotlinModule.Builder()
                .enable(KotlinFeature.NullIsSameAsDefault)
                .build()))
                .load(file, ConfigRadar.class);

        Properties buildProperties = new Properties();
        try (InputStream in = getClass().getResourceAsStream("/build.properties")) {
            if (in != null) {
                buildProperties.load(in);
            }
        }
        String version = buildProperties.getProperty("version");
        if (version != null) {
            properties.setBuildVersion(version);
        }
    }

    private Path getDefaultFile() throws IOException {
        Path localFile = Paths.get(CONFIG_FILE_NAME);
        if (!Files.exists(localFile)) {
            try {
                URL codePathUrl = RadarBackend.class.getProtectionDomain().getCodeSource()
                        .getLocation();
                String codePath = codePathUrl.toURI().getPath();
                String codeDir = codePath.substring(0, codePath.lastIndexOf('/') + 1);
                localFile = Paths.get(codeDir, CONFIG_FILE_NAME);
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

    @Override
    public PersistentStateStore getPersistentStateStore() throws IOException {
        if (getRadarProperties().getPersistencePath() != null) {
            Path persistenceDir = Paths.get(getRadarProperties().getPersistencePath());
            return new YamlPersistentStateStore(persistenceDir);
        } else {
            return null;
        }
    }
}

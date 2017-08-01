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

package org.radarcns.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.key.MeasurementKey;
import org.radarcns.monitor.DisconnectMonitor.MissingRecordsReport;

/** Store a state for a Kafka consumer. */
public class PersistentStateStore {
    private final File basePath;
    private final YamlConfigLoader loader;
    private static final char SEPARATOR = '#';
    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * State store that creates files at given directory. The directory will be created if it
     * does not exist.
     * @param basePath path to a directory.
     * @throws IOException if the directory is
     */
    public PersistentStateStore(File basePath) throws IOException {
        checkBasePath(basePath);

        this.basePath = basePath;
        this.loader = new YamlConfigLoader();
    }

    /** Check whether the base path can be made into a valid directory and is writable. */
    private static void checkBasePath(File basePath) throws IOException {
        if (basePath.exists()) {
            if (!basePath.isDirectory()) {
                throw new IOException("State path " + basePath.getAbsolutePath()
                        + " is not a directory");
            }
        } else if (!basePath.mkdirs()) {
            throw new IOException("Failed to set up persistent state store for the Kafka Monitor.");
        }

        File testFile = new File(basePath, ".check_base_path");
        // can write
        try (FileOutputStream fout = new FileOutputStream(testFile)) {
            fout.write(1);
        } catch (IOException ex) {
            throw new IOException("Cannot write files in directory " + basePath, ex);
        }
        //noinspection ResultOfMethodCallIgnored
        testFile.delete();
    }

    /** Retrieve a state from file. The default is returned if no state file is found.
     *
     * @throws IOException if file cannot be read or if the underlying file cannot be deserialized.
     */
    public <T> T retrieveState(String groupId, String clientId, T stateDefault)
            throws IOException {
        File consumerFile = getFile(groupId, clientId);
        if (!consumerFile.exists()) {
            return stateDefault;
        }
        @SuppressWarnings("unchecked")
        Class<? extends T> stateClass = (Class<? extends T>) stateDefault.getClass();
        return loader.load(consumerFile, stateClass);
    }

    /** Store a state to file. */
    public void storeState(String groupId, String clientId, Object value) throws IOException {
        loader.store(getFile(groupId, clientId), value);
    }

    /** File for given consumer. */
    private File getFile(String groupId, String clientId) {
        return new File(basePath, groupId + "_" + clientId + ".yml");
    }

    /**
     * Uniquely and efficiently serializes a measurement key. It can be deserialized with
     * {@link #stringToKey(String)}.
     * @param key key to serialize
     * @return unique serialized form
     */
    public static String measurementKeyToString(MeasurementKey key) {
        String userId = key.getUserId();
        String sourceId = key.getSourceId();
        StringBuilder builder = new StringBuilder(userId.length() + 5 + sourceId.length());
        escape(userId, builder);
        builder.append(SEPARATOR);
        escape(sourceId, builder);
        return builder.toString();
    }

    /**
     * serializes a MissingRecordsReport to a string. It can be deserialized with
     * {@link #stringToMissingRecordsReport(String)}.
     * @param reportedMissingValues key to serialize
     * @return unique serialized form
     */
    public static String missingRecordsReportToString(MissingRecordsReport reportedMissingValues)
        throws IOException {

        return objectMapper.writeValueAsString(reportedMissingValues);
    }

    /**
     * deserializes a string into a  MissingRecordsReport. It can be serialized with
     * {@link #stringToKey(String)}.
     * @param reportedMissingValues key to serialize
     * @return unique deserialized form
     */
    public static MissingRecordsReport stringToMissingRecordsReport(String reportedMissingValues)
        throws IOException {

        return objectMapper.readValue(reportedMissingValues , MissingRecordsReport.class);
    }


    private static void escape(String string, StringBuilder builder) {
        for (char c : string.toCharArray()) {
            if (c == '\\') {
                builder.append("\\\\");
            } else if (c == SEPARATOR) {
                builder.append('\\').append(SEPARATOR);
            } else {
                builder.append(c);
            }
        }
    }

    /**
     * Efficiently serializes a measurement key serialized with
     * {@link #measurementKeyToString(MeasurementKey)}.
     *
     * @param string serialized form
     * @return original measurement key
     */
    public static MeasurementKey stringToKey(String string) {
        StringBuilder builder = new StringBuilder(string.length());
        MeasurementKey key = new MeasurementKey();
        boolean hasSlash = false;
        for (char c : string.toCharArray()) {
            if (c == '\\') {
                if (hasSlash) {
                    builder.append(c);
                    hasSlash = false;
                } else {
                    hasSlash = true;
                }
            } else if (c == SEPARATOR) {
                if (hasSlash) {
                    builder.append(c);
                    hasSlash = false;
                } else {
                    key.setUserId(builder.toString());
                    builder.setLength(0);
                }
            } else {
                builder.append(c);
            }
        }
        key.setSourceId(builder.toString());
        return key;
    }
}

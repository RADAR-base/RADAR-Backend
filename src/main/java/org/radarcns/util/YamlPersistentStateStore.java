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
import org.radarcns.config.YamlConfigLoader;
import org.radarcns.kafka.ObservationKey;

/**
 * Store a state for a Kafka consumer. This uses a file storage, storing files to YAML format. It
 * uses Jackson for serialization and deserialization, so state objects must be serializable and
 * deserializable with this mechanism.
 */
public class YamlPersistentStateStore implements PersistentStateStore {
    private final File basePath;
    private final YamlConfigLoader loader;
    private static final char SEPARATOR = '#';

    /**
     * State store that creates files at given directory. The directory will be created if it
     * does not exist.
     * @param basePath path to a directory.
     * @throws IOException if the given directory is not writable for states.
     */
    public YamlPersistentStateStore(File basePath) throws IOException {
        checkBasePath(basePath);

        this.basePath = basePath;
        this.loader = new YamlConfigLoader();
    }

    /**
     * Check whether the base path can be made into a valid directory and is writable.
     *
     * @param basePath base path for the persistence store.
     * @throws IOException if the base path is not writable for states.
     */
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

    @Override
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

    @Override
    public void storeState(String groupId, String clientId, Object value) throws IOException {
        loader.store(getFile(groupId, clientId), value);
    }

    /** File for given consumer. */
    private File getFile(String groupId, String clientId) {
        return new File(basePath, groupId + "_" + clientId + ".yml");
    }

    @Override
    public String keyToString(ObservationKey key) {
        String projectId = key.getProjectId();
        String userId = key.getUserId();
        String sourceId = key.getSourceId();
        StringBuilder builder = new StringBuilder(
                (projectId == null ? 0 : projectId.length())
                        + userId.length() + 6 + sourceId.length());
        if (projectId != null) {
            escape(projectId, builder);
        }
        builder.append(SEPARATOR);
        escape(userId, builder);
        builder.append(SEPARATOR);
        escape(sourceId, builder);
        return builder.toString();
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

    @Override
    public ObservationKey stringToKey(String string) {
        StringBuilder builder = new StringBuilder(string.length());
        ObservationKey key = new ObservationKey();
        boolean hasSlash = false;
        int numFound = 0;
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
                    if (numFound == 0) {
                        numFound++;
                        if (builder.length() == 0) {
                            key.setProjectId(null);
                        } else {
                            key.setProjectId(builder.toString());
                            builder.setLength(0);
                        }
                    } else {
                        key.setUserId(builder.toString());
                        builder.setLength(0);
                    }
                }
            } else {
                builder.append(c);
            }
        }
        key.setSourceId(builder.toString());
        return key;
    }
}

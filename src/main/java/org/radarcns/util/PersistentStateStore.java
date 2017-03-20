/*
 * Copyright 2017 Kings College London and The Hyve
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
import java.io.IOException;
import org.radarcns.config.YamlConfigLoader;

public class PersistentStateStore {
    private final File basePath;
    private final YamlConfigLoader loader;

    public PersistentStateStore(File basePath) throws IOException {
        if (basePath.exists()) {
            if (!basePath.isDirectory()) {
                throw new IOException("State path " + basePath.getAbsolutePath()
                        + " is not a valid directory");
            }
        } else if (!basePath.mkdirs()) {
            throw new IOException("Failed to set up persistent state store for the Kafka Monitor.");
        }

        this.basePath = basePath;
        this.loader = new YamlConfigLoader();
    }

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

    public void storeState(String groupId, String clientId, Object value) throws IOException {
        loader.store(getFile(groupId, clientId), value);
    }

    private File getFile(String groupId, String clientId) {
        return new File(basePath, groupId + "_" + clientId + ".yml");
    }
}

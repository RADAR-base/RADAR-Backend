package org.radarcns.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;

public class PersistentStateStore {
    private final File basePath;
    private final ObjectMapper mapper;

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
        this.mapper = new ObjectMapper(new YAMLFactory());
    }

    public <T> T retrieveState(String groupId, String clientId, T stateDefault)
            throws IOException {
        File consumerFile = getFile(groupId, clientId);
        if (!consumerFile.exists()) {
            return stateDefault;
        }
        @SuppressWarnings("unchecked")
        Class<? extends T> stateClass = (Class<? extends T>) stateDefault.getClass();
        return mapper.readValue(consumerFile, stateClass);
    }

    public void storeState(String groupId, String clientId, Object value) throws IOException {
        File consumerFile = getFile(groupId, clientId);
        mapper.writeValue(consumerFile, value);
    }

    private File getFile(String groupId, String clientId) {
        return new File(basePath, groupId + "_" + clientId + ".yml");
    }
}

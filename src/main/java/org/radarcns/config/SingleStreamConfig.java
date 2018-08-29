package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nonnull;
import org.radarcns.config.RadarPropertyHandler.Priority;

public class SingleStreamConfig {
    @JsonProperty("class")
    private Class<?> streamClass;
    @JsonProperty
    private Map<String, String> properties;
    @JsonProperty
    private Priority priority = null;

    public void setStreamClass(Class<?> streamClass) {
        this.streamClass = streamClass;
    }

    public Class<?> getStreamClass() {
        return streamClass;
    }

    @Nonnull
    public Map<String, String> getProperties() {
        return properties != null ? properties : Collections.emptyMap();
    }

    @JsonSetter("priority")
    protected void setPriority(String priority) {
        this.priority = Priority.valueOf(priority.toUpperCase(Locale.US));
    }

    public void setDefaultPriority(Priority priority) {
        if (this.priority == null) {
            this.priority = priority;
        }
    }

    public Priority getPriority() {
        return priority != null ? priority : Priority.NORMAL;
    }
}

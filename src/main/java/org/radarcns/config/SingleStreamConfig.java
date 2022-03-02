package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.radarbase.stream.TimeWindowMetadata;
import org.radarcns.config.RadarPropertyHandler.Priority;

public class SingleStreamConfig {
    @JsonProperty("class")
    private Class<?> streamClass;
    @JsonProperty
    private Map<String, String> properties;
    @JsonProperty
    private Priority priority = null;

    @JsonProperty("time_windows")
    private Set<TimeWindowMetadata> timeWindows = null;

    @JsonProperty("use_reservoir_sampling")
    private Boolean useReservoirSampling = null;

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

    @SuppressWarnings("unused")
    public void setUseReservoirSampling(boolean useReservoirSampling) {
        this.useReservoirSampling = useReservoirSampling;
    }

    @SuppressWarnings("unused")
    public void setDefaultUseReservoirSampling(boolean useReservoirSampling) {
        if (this.useReservoirSampling == null) {
            this.useReservoirSampling = useReservoirSampling;
        }
    }

    public boolean isUseReservoirSampling() {
        return useReservoirSampling != null && useReservoirSampling;
    }

    public Priority getPriority() {
        return priority != null ? priority : Priority.NORMAL;
    }

    public Set<TimeWindowMetadata> getTimeWindows() {
        return timeWindows;
    }

    public void setTimeWindows(Set<TimeWindowMetadata> timeWindows) {
        this.timeWindows = timeWindows;
    }
}

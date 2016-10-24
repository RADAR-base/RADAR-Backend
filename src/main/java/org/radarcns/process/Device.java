package org.radarcns;

public class Device {
    private final String id;

    public Device(String id) {
        this.id = id;
    }

    public int hashCode() {
        return getId().hashCode();
    }

    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || !other.getClass().equals(this.getClass())) return false;

        return getId().equals(((Device) other).getId());
    }

    public String getId() {
        return id;
    }
}

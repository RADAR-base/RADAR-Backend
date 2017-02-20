package org.radarcns.config;

import java.util.List;
import org.radarcns.mock.MockDataConfig;

public class MockConfig {
    private List<MockDataConfig> data;

    public List<MockDataConfig> getData() {
        return data;
    }

    public void setData(List<MockDataConfig> data) {
        this.data = data;
    }
}

package org.radarcns.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RadarConfig extends AbstractConfig {

    public static final String SESSION_TIME_WINDOW = "session.length.ms";
    public static final long SESSION_TIME_WINDOW_DEFAULT = 10_000L;

    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    private static Logger logger = LoggerFactory.getLogger(RadarConfig.class);

    static {
        CONFIG_DEF.define(TopicGroup.all_in.getParam(), ConfigDef.Type.LIST, new ArrayList<>(),
                ConfigDef.Importance.LOW, "List of topics to consume.");
        CONFIG_DEF.define(SESSION_TIME_WINDOW, ConfigDef.Type.LONG, SESSION_TIME_WINDOW_DEFAULT,
                ConfigDef.Range.atLeast(0L), ConfigDef.Importance.LOW,
                "the time-window length to consider two events in a stream as part of the same session.");
    }

    //Enumerate all possible topics
    public enum TopicGroup {
        all_in("topic.list.in");

        private final String param;

        TopicGroup(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

    public RadarConfig(Map<?, ?> properties) {
        super(CONFIG_DEF, properties);
    }

    public static RadarConfig load(ClassLoader classLoader) {
        String configFile = "configuration/config.properties";
        Properties prop = new Properties();

        try (InputStream in = classLoader.getResourceAsStream(configFile)) {
            // load a properties file
            prop.load(in);
        } catch (IOException io) {
            logger.warn("Cannot read properties file {}. Using defaults.", configFile, io);
        }

        return new RadarConfig(prop);
    }

    /**
     * @param topic states which list of topics you want to retrieve
     * @return List containing topic names
     * @throws NullPointerException either the property has not been set up or the type does not
     *                              exist
     */
    public List<String> getTopicList(TopicGroup topic) throws NullPointerException {
        return getList(TopicGroup.all_in.getParam());
    }

    /**
     * Business function
     * @return the time-window length to consider two events as part of the same session, positive.
     * @throws NullPointerException the property has not been set up
     * @throws NumberFormatException the property is cannot be parsed as an integer.
     */
    public long getSessionTimeWindow() throws NullPointerException, NumberFormatException {
        return getLong(SESSION_TIME_WINDOW);
    }

    public void updateProperties(Properties properties, String... keys) {
        for (String key : keys) {
            properties.put(key, originals().get(key));
        }
    }
}

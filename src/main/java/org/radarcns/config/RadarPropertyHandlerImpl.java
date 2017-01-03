package org.radarcns.config;

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.annotation.Nonnull;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.radarcns.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/**
 * Java Singleton class for handling the yml config file. Implements @link{ RadarPropertyHandler}
 */
public class RadarPropertyHandlerImpl implements RadarPropertyHandler {

  private ConfigRadar properties;
  private KafkaProperty kafkaProperty;
  private static final String nameConfigFile = "radar.yml";
  private static final String nameLogFile = "backend.log";

  private static final Logger log = LoggerFactory.getLogger(RadarPropertyHandlerImpl.class);

  @Override
  public ConfigRadar getRadarProperties() {
    if (properties == null) {
      throw new IllegalStateException(
          "Properties cannot be accessed without calling load() first");
    }
    return properties;
  }

  @Override
  public void load(String pathFile) throws URISyntaxException, IOException {
    String message = "USER CONFIGURATION";

    if (properties != null) {
      throw new IllegalStateException("Properties class has been already loaded");
    }

    //If pathFile is null
    if (Strings.isNullOrEmpty(pathFile)) {
      URL pathUrl = Main.class.getProtectionDomain().getCodeSource().getLocation();
      pathFile = pathUrl.toURI().getPath();
      pathFile = pathFile.substring(0, pathFile.lastIndexOf('/') + 1) + nameConfigFile;
      message = "DEFAULT CONFIGURATION";
    }
    log.info("{}: loading config file at {}", message, pathFile);

    File file = new File(pathFile);

    message = null;
    if (!file.isFile()) {
      message = "Config file is invalid";
    }
    if (!file.exists()) {
      message = "Config file does not exist";
    }
    if (message != null) {
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    properties = loadConfigRadar(pathFile);

    //TODO: add check to validate configuration file. Remember
    //  - log path can be only null, all others have to be stated
    //  - mode can be standalone or high_performance
    //  - all thread priority must be bigger than 1

    if (!Strings.isNullOrEmpty(getRadarProperties().getLog_path())) {
      updateLog4jConfiguration(getRadarProperties().getLog_path());
    }

  }

  @Override
  public KafkaProperty getKafkaProperties() {
    if (this.kafkaProperty == null) {
      this.kafkaProperty = new KafkaProperty(getRadarProperties());
    }
    return this.kafkaProperty;
  }

  private ConfigRadar loadConfigRadar(@Nonnull String pathFile) throws IOException {

    try {
      Yaml yaml = new Yaml();
      InputStream in = Files.newInputStream(Paths.get(pathFile));
      return yaml.loadAs(in, ConfigRadar.class);
    } catch (IOException ex) {
      log.error("Impossible load properties", ex);
      throw ex;
    }
  }


  /**
   * @param logPath: new log file defined by the user
   * @throws IllegalArgumentException if logPath is null or is not a valid file
   */
  private void updateLog4jConfiguration(@Nonnull String logPath)
      throws IllegalArgumentException, IOException {
    String message = null;

    if (Strings.isNullOrEmpty(logPath)) {
      message = "Invalid log_path - check your configuration file";
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    if (logPath.lastIndexOf('/') != logPath.length() - 1) {
      logPath += "/";
    }

    File file = new File(logPath);
    if (!file.exists()) {
      message = "User Log path does not exist";
    } else if (!file.isDirectory()) {
      message = "User Log path is not a directory";
    }
    if (message != null) {
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    logPath += nameLogFile;

    java.util.Properties props = new java.util.Properties();
    try (InputStream in = this.getClass().getResourceAsStream("/log4j.properties")) {
      props.load(in);
    } catch (IOException e) {
      log.error("Error during configuration file loading", e);
      throw e;
    }
    props.setProperty("log4j.appender.file.File", logPath);
    LogManager.resetConfiguration();
    PropertyConfigurator.configure(props);

    log.info("Log path has been correctly configured to {}", logPath);
    log.info("All future messages will be redirected to the log file");
  }
}

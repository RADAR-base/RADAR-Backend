package org.radarcns.consumer.realtime;

import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.realtime.RealtimeConsumerConfig;
import org.radarcns.monitor.CombinedKafkaMonitor;
import org.radarcns.monitor.KafkaMonitor;

public class RealtimeInferenceConsumerFactory {

  public static KafkaMonitor createConsumersFor(RadarPropertyHandler handler) {
    return new CombinedKafkaMonitor(
        handler.getRadarProperties().getConsumerConfigs().stream()
            .map(c -> createConsumer(handler.getRadarProperties(), c)));
  }

  private static KafkaMonitor createConsumer(
      ConfigRadar radar, RealtimeConsumerConfig consumerConfig) {
    return new RealtimeInferenceConsumer(
        "realtime-group-" + consumerConfig.getTopic() + "-" + consumerConfig.getName(),
        consumerConfig.getTopic() + "-1",
        radar,
        consumerConfig);
  }
}

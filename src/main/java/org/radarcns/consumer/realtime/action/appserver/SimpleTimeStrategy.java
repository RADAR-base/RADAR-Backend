package org.radarcns.consumer.realtime.action.appserver;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Schedules the time based on current time with an optional added delay. */
public class SimpleTimeStrategy implements ScheduleTimeStrategy {

  private final Duration delay;

  public SimpleTimeStrategy(long delay, ChronoUnit unit) {
    this.delay = Duration.of(delay, unit);
  }

  @Override
  public Instant getScheduledTime() {
    return Instant.now().plus(delay);
  }
}

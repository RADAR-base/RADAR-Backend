package org.radarcns.consumer.realtime.action.appserver;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Schedules the time based on the next Time Of Day (eg- 09:00:00 means 9 am on the day) as
 * configured in the configuration file. If the time of day specified has passed for the current
 * day, it will schedule at the time of day the next day.
 */
public class TimeOfDayStrategy implements ScheduleTimeStrategy {

  private final String timeOfDay;
  private final String timezone;

  public TimeOfDayStrategy(String timeOfDay, String timezone) {
    if (timeOfDay == null || timeOfDay.isEmpty()) {
      throw new IllegalArgumentException(
          "The time of day is not provided. Cannot use this strategy.");
    }

    this.timeOfDay = timeOfDay;
    this.timezone = timezone == null ? "GMT" : timezone;
  }

  @Override
  public Instant getScheduledTime() {

    Instant now = Instant.now();

    LocalDate localDate = now.atZone(ZoneId.of(timezone)).toLocalDate();
    LocalDateTime ldt =
        localDate.atTime(LocalTime.parse(timeOfDay, DateTimeFormatter.ISO_LOCAL_TIME));

    // If time has already passed, schedule the next day
    if (ldt.isBefore(LocalDateTime.from(now))) {
      ldt = ldt.plusDays(1);
    }

    return ldt.toInstant(ZoneOffset.of(timezone));
  }
}

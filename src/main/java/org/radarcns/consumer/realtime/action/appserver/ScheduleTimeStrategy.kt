package org.radarcns.consumer.realtime.action.appserver;

import java.time.Instant;

/**
 * The time calculation strategy for scheduling the notification via the {@link
 * org.radarcns.consumer.realtime.action.ActiveAppNotificationAction}.
 *
 * <p>See {@link SimpleTimeStrategy}, {@link TimeOfDayStrategy}
 */
public interface ScheduleTimeStrategy {

  Instant getScheduledTime();
}

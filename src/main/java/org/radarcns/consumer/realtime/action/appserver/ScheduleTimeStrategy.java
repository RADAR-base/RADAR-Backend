package org.radarcns.consumer.realtime.action.appserver;

import java.time.Instant;

public interface ScheduleTimeStrategy {

  Instant getScheduledTime();
}

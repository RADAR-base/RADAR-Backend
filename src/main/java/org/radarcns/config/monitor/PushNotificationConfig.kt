package org.radarcns.config.monitor

import java.time.Duration

class PushNotificationConfig {
    var project: String? = null
    var condition: Condition? = null
    var questionnaire: List<String>? = null
    var ttlMargin: Duration = Duration.ofMinutes(5)
}

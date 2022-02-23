package org.radarcns.monitor.intervention

import java.time.Instant
import java.time.ZoneId

data class UserDetailCache(
    val language: String?,
    val zoneId: ZoneId?,
) {
    val fetchedAt: Instant = Instant.now()
}

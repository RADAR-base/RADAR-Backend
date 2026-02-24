package org.radarbase.kotlin.stream.phone

import org.junit.Assert.assertEquals
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.IOException

@RunWith(Parameterized::class)
class PlayStoreLookupTest(private val inputPackageName: String, private val expectedCategory: String?) {

    @Ignore("I think that PlayStore integration is broken.")
    @Test
    @Throws(IOException::class)
    fun fetchCategoryTest() {
        val result = PlayStoreLookup.fetchCategory(inputPackageName)
        assertEquals(expectedCategory, result.categoryName)
    }

    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "{index}: {0}={1}")
        fun data(): Collection<Array<Any?>> {
            return listOf(
                arrayOf("nl.nos.app", "NEWS_AND_MAGAZINES"),
                arrayOf("nl.thehyve.transmartclient", "MEDICAL"),
                arrayOf("com.twitter.android", "NEWS_AND_MAGAZINES"),
                arrayOf("com.facebook.katana", "SOCIAL"),
                arrayOf("com.nintendo.zara", "GAME_ACTION"),
                arrayOf("com.duolingo", "EDUCATION"),
                arrayOf("com.whatsapp", "COMMUNICATION"),
                arrayOf("com.alibaba.aliexpresshd", "SHOPPING"),
                arrayOf("com.google.android.wearable.app", "COMMUNICATION"),
                arrayOf("com.strava", "HEALTH_AND_FITNESS"),
                arrayOf("com.android.chrome", "COMMUNICATION"),
                arrayOf("com.google.android.youtube", "VIDEO_PLAYERS"),
                arrayOf("com.android.systemui", null),
                arrayOf("abc.abc", null)
            )
        }
    }
}

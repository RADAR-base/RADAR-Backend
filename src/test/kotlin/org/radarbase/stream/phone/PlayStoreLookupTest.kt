package org.radarbase.stream.phone

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.stream.Stream

@RunWith(Parameterized::class)
class PlayStoreLookupTest {

    @Disabled("I think that PlayStore integration is broken.")
    @ParameterizedTest(name = "{index}: {0}={1}")
    @MethodSource("provideTestData")
    fun fetchCategoryTest(inputPackageName: String, expectedCategory: String?) {
        val result = PlayStoreLookup.fetchCategory(inputPackageName)
        assertEquals(expectedCategory, result.categoryName)
    }

    companion object {
        @JvmStatic
        fun provideTestData(): Stream<Arguments> {
            return Stream.of(
                Arguments.of("nl.nos.app", "NEWS_AND_MAGAZINES"),
                Arguments.of("nl.thehyve.transmartclient", "MEDICAL"),
                Arguments.of("com.twitter.android", "NEWS_AND_MAGAZINES"),
                Arguments.of("com.facebook.katana", "SOCIAL"),
                Arguments.of("com.nintendo.zara", "GAME_ACTION"),
                Arguments.of("com.duolingo", "EDUCATION"),
                Arguments.of("com.whatsapp", "COMMUNICATION"),
                Arguments.of("com.alibaba.aliexpresshd", "SHOPPING"),
                Arguments.of("com.google.android.wearable.app", "COMMUNICATION"),
                Arguments.of("com.strava", "HEALTH_AND_FITNESS"),
                Arguments.of("com.android.chrome", "COMMUNICATION"),
                Arguments.of("com.google.android.youtube", "VIDEO_PLAYERS"),
                Arguments.of("com.android.systemui", null),
                Arguments.of("abc.abc", null),
            )
        }
    }
}

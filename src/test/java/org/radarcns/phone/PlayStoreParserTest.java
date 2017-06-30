package org.radarcns.phone;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class PlayStoreParserTest {
    @Parameters(name = "{index}: {0}={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "nl.nos.app", "NEWS_AND_MAGAZINES" }
                ,{ "nl.thehyve.transmartclient", "MEDICAL" }
                ,{ "com.twitter.android", "NEWS_AND_MAGAZINES" }
                ,{ "com.facebook.katana", "SOCIAL" }
                ,{ "com.nintendo.zara", "GAME_ACTION" }
                ,{ "com.duolingo", "EDUCATION" }
                ,{ "com.whatsapp", "COMMUNICATION" }
                ,{ "com.alibaba.aliexpresshd", "SHOPPING" }
                ,{ "com.google.android.wearable.app", "COMMUNICATION" }
                ,{ "com.strava", "HEALTH_AND_FITNESS" }
                ,{ "com.android.chrome", "COMMUNICATION" }
                ,{ "com.google.android.youtube", "VIDEO_PLAYERS" }
                ,{ "com.withings.wiscale2", "HEALTH_AND_FITNESS" }
        });
    }

    private String fInputPackageName;

    private String fExpectedCategory;

    public PlayStoreParserTest(String input, String expected) {
        fInputPackageName= input;
        fExpectedCategory= expected;
    }

    @Test
    public void test() {
        String result = PlayStoreLookup.fetchCategory(fInputPackageName);
        assertEquals(fExpectedCategory, result);
    }
}

/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.stream.phone;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.radarcns.stream.phone.PlayStoreLookup.AppCategory;

@RunWith(Parameterized.class)
public class PlayStoreLookupTest {
    @Parameters(name = "{index}: {0}={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "nl.nos.app", "NEWS_AND_MAGAZINES" }
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
                ,{ "com.android.systemui", null }
                ,{ "abc.abc", null }
        });
    }

    private final String fInputPackageName;
    private final String fExpectedCategory;

    public PlayStoreLookupTest(String input, String expected) {
        fInputPackageName = input;
        fExpectedCategory = expected;
    }

    @Test
    public void fetchCategoryTest() throws IOException {
        AppCategory result = PlayStoreLookup.fetchCategory(fInputPackageName);
        assertEquals(fExpectedCategory, result.getCategoryName());
    }
}

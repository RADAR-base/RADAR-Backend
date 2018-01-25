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
import static org.junit.Assert.assertNull;

import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;
import org.radarcns.stream.phone.PlayStoreLookup.AppCategory;

/**
 * Created by joris on 13/07/2017.
 */
public class PlayStoreCategoryParserTest {
    private static final String BASE_URL = "https://play.google.com/store/apps/details?id=nl.thehyve.transmartclient";

    @Test
    public void getCategoryFromDocument() throws Exception {
        Document doc;
        try (InputStream stream = getClass().getResourceAsStream("transmart_app.html.gz");
                InputStream gzipStream = new GZIPInputStream(stream)) {

            doc = Jsoup.parse(gzipStream, "UTF-8", BASE_URL);
        }

        AppCategory category = PlayStoreLookup.getCategoryFromDocument(
                doc, "nl.thehyve.transmartclient");
        assertEquals("MEDICAL", category.getCategoryName());
    }

    @Test
    public void getCategoryFromDocumentNoCategory() throws Exception {
        Document doc;
        try (InputStream stream = getClass().getResourceAsStream("transmart_app_no_category.html.gz");
                InputStream gzipStream = new GZIPInputStream(stream)) {

            doc = Jsoup.parse(gzipStream, "UTF-8", BASE_URL);
        }

        AppCategory category = PlayStoreLookup.getCategoryFromDocument(
                doc, "nl.thehyve.transmartclient");
        assertNull(category.getCategoryName());
    }

    @Test
    public void getCategoryFromDocumentBroken() throws Exception {
        Document doc;
        try (InputStream stream = getClass().getResourceAsStream("transmart_app_broken.html.gz");
                InputStream gzipStream = new GZIPInputStream(stream)) {

            doc = Jsoup.parse(gzipStream, "UTF-8", BASE_URL);
        }

        AppCategory category = PlayStoreLookup.getCategoryFromDocument(
                doc, "nl.thehyve.transmartclient");
        assertNull(category.getCategoryName());
    }
}

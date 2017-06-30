package org.radarcns.phone;

/*
 * Copyright 2017 The Hyve
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

import java.io.IOException;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.radarcns.phone.streams.PhoneUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PlayStoreLookup {
    private static final Logger log = LoggerFactory.getLogger(PhoneUsage.class);

    private static final String URL_PLAY_STORE_APP_DETAILS = "https://play.google.com/store/apps/details?id=";
    private static final String CATEGORY_ANCHOR_SELECTOR = "a.document-subtitle.category";

    private PlayStoreLookup() {}

    /**
     * Fetches the app category by parsing the Play Store
     * Returning empty string can mean:
     * - Page can't be retrieved because app is not listed in play store
     * - Category element on play store is not available
     * - URL can't be parsed
     * @param packageName name of the package as registered in the play store
     * @return category as given by the play store
     */
    public static String fetchCategory(String packageName) {
        String category = null;
        String url = "";
        try {
            url = createAppDetailsUrl(packageName);

            Document doc = Jsoup.connect(url).get();

            // If multiple category anchors, get first
            Element categoryElement = doc.select(CATEGORY_ANCHOR_SELECTOR).first();

            boolean fetchSuccess = false;
            if (categoryElement != null) {
                String href = categoryElement.attr("href");
                if (href != null) {
                    category = getCategoryIdFromUrl(href);
                    fetchSuccess = true;
                }
            }
            if (!fetchSuccess) {
                throw new IOException("Element containing category could not be found");
            }
        } catch (HttpStatusException ex) {
            log.warn("Could not connect to " + url);
        } catch (IOException ex) {
            log.warn("Could not find category of " + packageName + ": " + ex.toString());
        }

        if (category == null) {
            return "";
        }

        return category;
    }

    private static String createAppDetailsUrl(String packageName) {
        return URL_PLAY_STORE_APP_DETAILS + packageName;
    }

    private static String getCategoryIdFromUrl(String categoryUrl) {
        String[] urlSplit = categoryUrl.split("/");
        return urlSplit[urlSplit.length - 1];
    }

}


package org.radarcns.net;

import org.radarcns.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

public class HttpClient {
    private final static Logger logger = LoggerFactory.getLogger(HttpClient.class);

    public static HttpResponse request(URL url, String method, String data) throws IOException {
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        String responseContent = null;
        try {
            urlConnection.setRequestMethod(method);
            urlConnection.setRequestProperty("Accept", "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json");
            urlConnection.addRequestProperty("Accept-Encoding", "identity");

            if (Objects.equals(method, "HEAD")) {
                urlConnection.setDoOutput(false);
                urlConnection.setDoInput(false);
                urlConnection.connect();
            } else {
                urlConnection.setDoInput(true);
                if (data == null) {
                    urlConnection.setDoOutput(false);
                    urlConnection.connect();
                } else {
                    byte[] bytes = data.getBytes("UTF-8");
                    urlConnection.setRequestProperty("Content-Type", "application/vnd.kafka.avro.v1+json; charset=utf-8");
                    urlConnection.setDoOutput(true);
                    urlConnection.setChunkedStreamingMode(0);
                    urlConnection.connect();

                    try (OutputStream out = urlConnection.getOutputStream()) {
                        out.write(bytes);
                    }
                }
                if (urlConnection.getResponseCode() < 400) {
                    try (InputStream in = new BufferedInputStream(urlConnection.getInputStream())) {
                        responseContent = IO.readInputStream(in);
                    }
                } else {
                    try (InputStream in = new BufferedInputStream(urlConnection.getErrorStream())) {
                        responseContent = IO.readInputStream(in);
                    }
                }
            }

            return new HttpResponse(urlConnection.getResponseCode(), urlConnection.getHeaderFields(), responseContent);
        } catch (IOException ex) {
            ex.printStackTrace();
            logger.warn("Failed HTTP {} request to {} (status code {}, response {}): {} {}", method, url, urlConnection.getResponseCode(), responseContent, ex.toString(), data, ex);
            throw ex;
        } finally {
            urlConnection.disconnect();
        }
    }
}

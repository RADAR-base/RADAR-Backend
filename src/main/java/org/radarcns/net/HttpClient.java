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

public class HttpClient {
    private final static Logger logger = LoggerFactory.getLogger(HttpClient.class);

    public static HttpResponse request(URL url, String method, String data) throws IOException {
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        try {
            urlConnection.setDoOutput(true);
            urlConnection.setChunkedStreamingMode(0);
            urlConnection.setRequestMethod(method);
            urlConnection.setRequestProperty("Accept", "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json");
            urlConnection.setRequestProperty("Content-Type", "application/vnd.kafka.avro.v1+json; charset=utf-8");

            try (OutputStream out = new BufferedOutputStream(urlConnection.getOutputStream())) {
                out.write(data.getBytes("UTF-8"));
            }
            String outputContent;
            try (InputStream in = new BufferedInputStream(urlConnection.getInputStream())) {
                outputContent = IO.readInputStream(in);
            } catch (IOException ex) {
                outputContent = null;
            }
            return new HttpResponse(urlConnection.getResponseCode(), urlConnection.getHeaderFields(), outputContent);
        } catch (IOException ex) {
            logger.warn("Failed HTTP {} request to {} ({}): {} {}", method, url, data, ex.toString(), ex.getMessage());
            throw ex;
        } finally {
            urlConnection.disconnect();
        }
    }
}

package org.radarcns.collect.rest;

import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class StringEncoder implements AvroEncoder<String> {
    @Override
    public String encode(String object, Schema schema) throws IOException {
        if (schema.getType() != Schema.Type.STRING) {
            throw new IllegalArgumentException("Cannot encode String with a different type than STRING.");
        }
        return new ObjectMapper().writeValueAsString(object);
    }
}

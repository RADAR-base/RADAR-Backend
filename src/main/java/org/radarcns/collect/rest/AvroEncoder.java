package org.radarcns.collect.rest;

import org.apache.avro.Schema;

import java.io.IOException;

public interface AvroEncoder<T> {
    String encode(T object, Schema schema) throws IOException;
}

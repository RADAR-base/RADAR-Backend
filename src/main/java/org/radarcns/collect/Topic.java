package org.radarcns.collect;

import org.apache.avro.Schema;

import java.io.InputStream;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Topic {
    private final int hertz;
    private final String name;
    private Schema schema;
    private final static Logger logger = LoggerFactory.getLogger(Topic.class);

    public Topic(String name, int hertz) {
        this.name = name;
        this.hertz = hertz;
        this.schema = null;
    }

    public Schema getSchema() {
        if (schema == null) {
            logger.debug("Retrieving schema for topic {}", getName());
            InputStream schemaStream = Topic.class.getResourceAsStream("schema/" + getName() + ".json");
            Scanner s = new Scanner(schemaStream).useDelimiter("\\A");
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(s.hasNext() ? s.next() : "");
        }
        return schema;
    }

    public int getHertz() {
        return hertz;
    }

    public String getName() {
        return name;
    }
}

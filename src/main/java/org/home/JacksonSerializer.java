package org.home;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Writer;

/**
 * Created by oliv on 23/05/2016.
 */
public class JacksonSerializer {

    public JacksonSerializer() {
        serializer = new ObjectMapper();
    }

    private ObjectMapper serializer;

    public void serialize(Writer writer,
                          Object object) throws IOException {
        serializer.writeValue(writer, object);
    }
}

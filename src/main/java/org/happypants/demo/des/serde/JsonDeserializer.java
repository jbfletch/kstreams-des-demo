package org.happypants.demo.des.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer implements Deserializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    JsonDeserializer() {

    }

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {
        // nothing to configure
    }

    @Override
    public JsonNode deserialize(final String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            return this.objectMapper.readTree(bytes);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
    }
}
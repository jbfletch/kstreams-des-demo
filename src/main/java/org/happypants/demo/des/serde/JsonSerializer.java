package org.happypants.demo.des.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


class JsonSerializer implements Serializer<JsonNode> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    JsonSerializer() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, Headers headers, JsonNode data) {
        if (data == null) {
            return null;
        } else {
            try {
                return this.objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new SerializationException();
            }
        }
    }

        public byte[] serialize(String topic, JsonNode data) {
            if (data == null) {
                return null;
            } else {
                try {
                    return this.objectMapper.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new SerializationException();
                }
            }
    }
    public void close() {
    }
}
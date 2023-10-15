package com.substack.dataengineering;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;
import java.util.Map;

public class JsonPOJOSerializer implements Serializer<Map<String, Object>>, Deserializer<Map<String, Object>> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public Map<String, Object> deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        Map<String, Object> data;
        try {
            data = objectMapper.readValue(bytes, Map.class);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize", e);
        }
        return data;
    }

    @Override
    public byte[] serialize(String topic, Map<String, Object> data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}

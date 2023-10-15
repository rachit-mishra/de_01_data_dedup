package com.substack.dataengineering;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Map;

public class JsonPOJOSerde implements Serde<Map<String, Object>> {

    private final Serde<Map<String, Object>> inner;

    public JsonPOJOSerde() {
        inner = Serdes.serdeFrom(new JsonPOJOSerializer(), new JsonPOJOSerializer());
    }

    @Override
    public Serializer<Map<String, Object>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<Map<String, Object>> deserializer() {
        return inner.deserializer();
    }
}

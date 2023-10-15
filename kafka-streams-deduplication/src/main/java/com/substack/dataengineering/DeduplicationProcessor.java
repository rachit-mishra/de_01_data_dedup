package com.substack.dataengineering;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

public class DeduplicationProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Initial input as <String, String>
        KStream<String, String> rawInput = builder.stream("dedup_weather_topic", Consumed.with(Serdes.String(), Serdes.String()));

        // Let's write the raw input directly to another topic (raw_weather_data)
        rawInput.to("weather_data_raw");

        // ObjectMapper for converting JSON string to Map
        ObjectMapper mapper = new ObjectMapper();

        // Convert rawInput values from JSON strings to Maps
        KStream<String, Map<String, Object>> input = rawInput.mapValues(value -> {
            try {
                return mapper.readValue(value, Map.class);
            } catch (Exception e) {
                // Log this or handle it according to your needs
                return new HashMap<>();
            }
        });

        // Deduplication logic
        KStream<String, Map<String, Object>> deduplicated = input
            .selectKey((key, value) -> value.get("sensor_id").toString() + "_" + value.get("timestamp").toString())
            .groupByKey(Grouped.with(Serdes.String(), new JsonPOJOSerde()))
            .reduce((aggValue, newValue) -> newValue)  // Always keep the latest value for a particular key
            .toStream();

        deduplicated.to("weather_data_dedup",Produced.with(Serdes.String(), new JsonPOJOSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}

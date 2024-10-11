package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaFlinkJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:29092");
        properties.setProperty("group.id", "flink-group");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("raw-topic1", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);

        // Process the stream
        DataStream<String> processedStream = stream.flatMap(new FlatMapFunction<String, String>() {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // Parse the JSON message
                JsonNode jsonNode = objectMapper.readTree(value);
                int messageCount = jsonNode.get("message_count").asInt();

                // Process the message
                out.collect(String.valueOf(messageCount));
            }
        });

        // Create a Kafka producer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("processed-topic1", new SimpleStringSchema(), properties);
        processedStream.addSink(producer);

        // Execute the Flink job
        env.execute("Kafka Flink Job");
    }
}
package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaFlinkJob.class);

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:29092");
        properties.setProperty("group.id", "flink-group");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("raw-topic", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);

        // Process the stream
        DataStream<String> processedStream = stream.flatMap(new FlatMapFunction<String, String>() {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // Parse the JSON message
                JsonNode jsonNode = objectMapper.readTree(value);
                int messageCount = jsonNode.get("message_count").asInt();

                // Log the received message
                LOG.info("Received message: {}", messageCount);

                //--------------------------------------------------------------------------------|
                //--------------------------------------------------------------------------------|
                // This is where the privacy enhancing transformation would be implemented

                // floor the message count to the nearest 10
                messageCount = messageCount - (messageCount % 100);

                //--------------------------------------------------------------------------------|
                //--------------------------------------------------------------------------------|

                // Create a new JSON object with the same format
                ObjectNode outputJson = objectMapper.createObjectNode();
                outputJson.put("message_count", messageCount);

                // Collect the JSON string
                out.collect(outputJson.toString());
            }
        });

        // Create a Kafka producer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("processed-topic", new SimpleStringSchema(), properties);
        processedStream.addSink(producer);

        // Execute the Flink job
        env.execute("Kafka Flink Job");
    }
}
package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
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
                int resp = jsonNode.get("resp").asInt();
                int bps = jsonNode.get("bps").asInt();
                int pulse = jsonNode.get("pulse").asInt();
                double temp = jsonNode.get("temp").asDouble();

                // Log the received message
                LOG.info("[flink-job] Received message: {}", jsonNode);

                //--------------------------------------------------------------------------------|
                //--------------------------------------------------------------------------------|
                // This is where the privacy enhancing transformation would be implemented
                // The transformation would be applied to the variables resp, bps, pulse, and temp
                // The transformed values would be stored in the same variables as their NEWS2 counterparts
                // The transformed values would be used to calculate the NEWS2 score
                
                // Respiration rate per minute
                int newsResp = resp;
                // Systolic blood pressure
                int newsBps = bps;
                // Pulse rate per minute
                int newsPulse = pulse;
                // Temperature in degrees Celsius
                double newsTemp = temp;

                // Convert measurements into NEWS2 scores
                // 1. Respiration rate per minute
                int newsRespScore;
                if (newsResp >= 0 && newsResp <= 8) {
                    newsRespScore = 3;
                } else if (newsResp >= 9 && newsResp <= 11) {
                    newsRespScore = 1;
                } else if (newsResp >= 12 && newsResp <= 20) {
                    newsRespScore = 0;
                } else if (newsResp >= 21 && newsResp <= 24) {
                    newsRespScore = 3;
                } else if (newsResp >= 25) {
                    newsRespScore = 3;
                } else {
                    newsRespScore = -1;
                }

                // 2. Systolic blood pressure
                int newsBpsScore;
                if (newsBps >= 0 && newsBps <= 90) {
                    newsBpsScore = 3;
                } else if (newsBps >= 91 && newsBps <= 100) {
                    newsBpsScore = 2;
                } else if (newsBps >= 101 && newsBps <= 110) {
                    newsBpsScore = 1;
                } else if (newsBps >= 111 && newsBps <= 219) {
                    newsBpsScore = 0;
                } else if (newsBps >= 220) {
                    newsBpsScore = 3;
                } else {
                    newsBpsScore = -1;
                }

                // 3. Pulse rate per minute
                int newsPulseScore;
                if (newsPulse >= 0 && newsPulse <= 40) {
                    newsPulseScore = 3;
                } else if (newsPulse >= 41 && newsPulse <= 50) {
                    newsPulseScore = 1;
                } else if (newsPulse >= 51 && newsPulse <= 90) {
                    newsPulseScore = 0;
                } else if (newsPulse >= 91 && newsPulse <= 110) {
                    newsPulseScore = 1;
                } else if (newsPulse >= 111 && newsPulse <= 130) {
                    newsPulseScore = 2;
                } else if (newsPulse >= 131) {
                    newsPulseScore = 3;
                } else {
                    newsPulseScore = -1;
                }

                // 4. Temperature in degrees Celsius
                int newsTempScore;
                if (newsTemp >= 0 && newsTemp <= 35.0) {
                    newsTempScore = 3;
                } else if (newsTemp > 35.0 && newsTemp <= 36.0) {
                    newsTempScore = 0;
                } else if (newsTemp > 36.0 && newsTemp <= 38.0) {
                    newsTempScore = 1;
                } else if (newsTemp > 38.0 && newsTemp <= 39.0) {
                    newsTempScore = 2;
                } else if (newsTemp > 39.0) {
                    newsTempScore = 3;
                } else {
                    newsTempScore = -1;
                }
                

                //--------------------------------------------------------------------------------|
                //--------------------------------------------------------------------------------|

                // Create a new JSON object with the same format
                ObjectNode outputJson = objectMapper.createObjectNode();
                outputJson.put("resp", newsRespScore);
                outputJson.put("bps", newsBpsScore);
                outputJson.put("pulse", newsPulseScore);
                outputJson.put("temp", newsTempScore);

                // Collect the JSON string
                out.collect(outputJson.toString());

                // Log the processed message
                LOG.info("[flink-job] Processed message: {}", outputJson);
            }
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("processed-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        processedStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("Kafka Flink Job");
    }
}
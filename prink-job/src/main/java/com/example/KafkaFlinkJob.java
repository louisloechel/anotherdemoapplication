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
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.Gauge;

import java.util.Properties;

public class KafkaFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaFlinkJob.class);

    // Create Prometheus gauges for the NEWS2 variables
    private static final Gauge respGauge = Gauge.build()
        .name("news_resp_score")
        .help("Respiration rate per minute")
        .register();
    private static final Gauge bpsGauge = Gauge.build()
        .name("news_bps_score")
        .help("Systolic blood pressure")
        .register();
    private static final Gauge pulseGauge = Gauge.build()
        .name("news_pulse_score")
        .help("Pulse rate per minute")
        .register();
    private static final Gauge tempGauge = Gauge.build()
        .name("news_temp_score")
        .help("Temperature in degrees Celsius")
        .register();

    public static void main(String[] args) throws Exception {
        // Start Prometheus HTTP server
        HTTPServer server = new HTTPServer(8002);
        
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:29092");
        properties.setProperty("group.id", "flink-group");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("processed-topic", new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);

        // Process the stream
        DataStream<String> processedStream = stream.flatMap(new FlatMapFunction<String, String>() {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // Parse the JSON string
                JsonNode jsonNode = objectMapper.readTree(value);

                // Check if the fields are present and not null before parsing
                int resp = jsonNode.hasNonNull("resp") ? jsonNode.get("resp").asInt() : -1; // Default value -1 if not present
                int bps = jsonNode.hasNonNull("bps") ? jsonNode.get("bps").asInt() : -1; // Default value -1 if not present
                int pulse = jsonNode.hasNonNull("pulse") ? jsonNode.get("pulse").asInt() : -1; // Default value -1 if not present
                double temp = jsonNode.hasNonNull("temp") ? jsonNode.get("temp").asDouble() : -1.0; // Default value -1.0 if not present

                // Log the received message
                LOG.info("[prink-job] Received message: {}", jsonNode);

                //--------------------------------------------------------------------------------|
                //--------------------------------------------------------------------------------|
                // This is where the privacy enhancing transformation would be implemented
                // The transformation would be applied to the variables resp, bps, pulse, and temp
                // The transformed values would be stored in the same variables as their NEWS2 counterparts
                // The transformed values would be used to calculate the NEWS2 score
                
                // Assuming these values are computed dynamically
                int newsRespScore = computeRespScore(value);
                int newsBpsScore = computeBpsScore(value);
                int newsPulseScore = computePulseScore(value);
                double newsTempScore = computeTempScore(value);

                // Set the gauges to the computed values
                respGauge.set(newsRespScore);
                bpsGauge.set(newsBpsScore);
                pulseGauge.set(newsPulseScore);
                tempGauge.set(newsTempScore);

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

                LOG.info("[prink-job] Processed message: {}", outputJson);
            }
        });

        // Create a Kafka producer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("prink-topic", new SimpleStringSchema(), properties);
        processedStream.addSink(producer);

        // Execute the Prink job
        env.execute("Kafka Prink Job");
    }

    // Placeholder methods for computing the scores
    private static int computeRespScore(String value) {
        // Implement your logic to compute the respiration score
        return 3; // Example value
    }

    private static int computeBpsScore(String value) {
        // Implement your logic to compute the blood pressure score
        return 2; // Example value
    }

    private static int computePulseScore(String value) {
        // Implement your logic to compute the pulse score
        return 1; // Example value
    }

    private static double computeTempScore(String value) {
        // Implement your logic to compute the temperature score
        return 0.0; // Example value
    }
}
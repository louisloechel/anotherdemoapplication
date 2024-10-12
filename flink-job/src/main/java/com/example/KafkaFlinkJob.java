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
                // The JSON message is expected to have the following format:
                // {
                //     "eformdataid": "57906",
                //     "date": "07-Jan",
                //     "time": "11:18",
                //     "resp": "12", <<<<<<<  
                //     "oxcode": "",
                //     "oxpercent": "",
                //     "oxflow": "",
                //     "oxcodename": "",
                //     "oxsat": "95",
                //     "oxsatscale": "1",
                //     "bps": "175", <<<<<<<
                //     "bpd": "93",
                //     "pulse": "100", <<<<<<<
                //     "acvpu": "Alert",
                //     "temp": "37.1", <<<<<<<
                //     "newstotal": "2",
                //     "newsrepeat": "4 hours",
                //     "userinitials": "RB",
                //     "username": "Rhidian Bramley",
                //     "userid": "532",
                //     "escalation": "No"
                //   }
                JsonNode jsonNode = objectMapper.readTree(value);
                int resp = jsonNode.get("resp").asInt();
                int bps = jsonNode.get("bps").asInt();
                int pulse = jsonNode.get("pulse").asInt();
                double temp = jsonNode.get("temp").asDouble();

                // Log the received message
                LOG.info("Received message: {}", jsonNode);

                //--------------------------------------------------------------------------------|
                //--------------------------------------------------------------------------------|
                // This is where the privacy enhancing transformation would be implemented
                // The transformation would be applied to the variables resp, bps, pulse, and temp
                // The transformed values would be stored in the same variables as their NEWS2 counterparts
                // The transformed values would be used to calculate the NEWS2 score
                
                // Respiration rate per minute
                int newsResp = resp;
                // Systolic blood pressure
                int newsBps = -1; //bps;
                // Pulse rate per minute
                int newsPulse = -1; //pulse;
                // Temperature in degrees Celsius
                double newsTemp = -1; //temp;

                // Convert measurements into NEWS2 scores
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
                

                //--------------------------------------------------------------------------------|
                //--------------------------------------------------------------------------------|

                // Create a new JSON object with the same format
                ObjectNode outputJson = objectMapper.createObjectNode();
                outputJson.put("resp", newsResp);
                outputJson.put("bps", newsBps);
                outputJson.put("pulse", newsPulse);
                outputJson.put("temp", newsTemp);

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
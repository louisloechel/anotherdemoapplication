import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class FlinkKafkaProcessor {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer properties
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "kafka:9092");
        consumerProperties.setProperty("group.id", "flink-group");

        // Create Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "my-topic",
                new SimpleStringSchema(),
                consumerProperties
        );

        // Kafka producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "kafka:9092");

        // Create Kafka producer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "processed-topic",
                new SimpleStringSchema(),
                producerProperties
        );

        // Read messages from Kafka
        DataStream<String> inputStream = env.addSource(consumer);

        // Process the stream
        DataStream<String> processedStream = inputStream
                .map(value -> {
                    // Assuming the message is in the format "Message ID"
                    String[] parts = value.split(" ");
                    int id = Integer.parseInt(parts[1]);
                    return id;
                })
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.CountWindows.of(10))
                .reduce((a, b) -> a + b)
                .map(sum -> {
                    int average = sum / 10;
                    // Replace IDs with the average
                    return "Message " + average;
                });

        // Write the processed data back to Kafka
        processedStream.addSink(producer);

        // Execute the Flink job
        env.execute("Kafka Flink Processing Job");
    }
}
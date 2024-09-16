from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json

env = StreamExecutionEnvironment.get_execution_environment()

# Kafka consumer
consumer_properties = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'flink-group'
}

kafka_consumer = FlinkKafkaConsumer(
    topics='topicA',
    deserialization_schema=SimpleStringSchema(),
    properties=consumer_properties
)

# Kafka producer
producer_properties = {'bootstrap.servers': 'kafka:9092'}

kafka_producer = FlinkKafkaProducer(
    topic='topicB',
    serialization_schema=SimpleStringSchema(),
    producer_config=producer_properties
)

# Data processing pipeline
data_stream = env.add_source(kafka_consumer) \
    .map(lambda x: json.loads(x)) \
    .map(lambda x: json.dumps({
        'id': x['id'],
        'name': x['name'],
        'value': float(x['value']) * 1.8 + 32  # Celsius to Fahrenheit
    }))

data_stream.add_sink(kafka_producer)

env.execute('Flink Kafka Stream Processing')
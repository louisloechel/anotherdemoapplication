from pyflink.datastream import StreamExecutionEnvironment # type: ignore
from pyflink.table import StreamTableEnvironment # type: ignore
from pyflink.table.descriptors import Schema, Json # type: ignore

# --- create output Kafka topic ---
# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092'
}

# Define the topic name
topic_name = 'processed-topic'

# Create the topic if it doesn't exist
def create_topic(topic_name):
    topic_list = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
    fs = admin_client.create_topics(topic_list)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

create_topic(topic_name)

# --- create Flink environment ---
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

## Define Kafka source
t_env.connect(
    Kafka()
    .version("universal")
    .topic("my-topic")
    .start_from_latest()
    .property("bootstrap.servers", "localhost:9092")
).with_format(
    Json()
).with_schema(
    Schema().field("field1", "STRING").field("field2", "INT")
).create_temporary_table("kafka_source")

# --- Transformations --- 
# Get 'message_count' from input table
result = t_env.from_path('input_table').select('message_count')
# Perfrom some operation on 'message_count'
result = result.add(1000)
# Write the result to a Kafka sink
print("\nWriting to Kafka sink")
print(result)
result.execute_insert("processed-topic")
# Execute Flink job
env.execute("Python Flink Job")
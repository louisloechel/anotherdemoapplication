FROM --platform=linux/amd64 flink:1.14.0 

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Install Kafka connector dependencies
RUN mkdir -p /opt/flink/usrlib
RUN wget -P /opt/flink/usrlib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka_2.12/1.14.0/flink-connector-kafka_2.12-1.14.0.jar

# install requirements
RUN pip install apache-flink confluent_kafka pyflink
#--no-cache-dir -r requirements.txt

# copy the source code
COPY . /opt/flink/

# run the application
CMD ["python3", "pet.py"]
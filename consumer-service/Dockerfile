# Use an official Python runtime as the base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any necessary Python dependencies
RUN pip install confluent_kafka prometheus_client

# Run the consumer when the container launches
CMD ["python", "consumer.py"]
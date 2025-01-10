# Use an official Python runtime as the base image
FROM --platform=linux/amd64 python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install any necessary Python dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt
ENV PYTHONUNBUFFERED=1

# Run the consumer when the container launches
CMD ["python", "consumer.py"]
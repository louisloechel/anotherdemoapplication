# Stage 1: Build the Flink job
FROM maven:3.8.1-openjdk-11 AS builder

# Set the working directory
WORKDIR /flink-job

# Copy the Maven project files
COPY pom.xml .
COPY src ./src

# Build the Maven project
RUN mvn clean package

# Stage 2: Create the final image
FROM flink:latest

# Set the working directory
WORKDIR /flink-job

# Copy the JAR file from the builder stage
COPY --from=builder /flink-job/target/flink-job-1.0-SNAPSHOT.jar /flink-job/flink-job.jar

# Set the entrypoint to run the Flink job
# ENTRYPOINT ["flink", "run", "/flink-job/flink-job.jar"]
ENTRYPOINT ["flink", "run", "-m", "flink-jobmanager:8081", "/flink-job/flink-job.jar"]
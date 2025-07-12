# Stage 1: Build the Flink job
FROM maven:3.8.1-openjdk-11 AS builder

# Set the working directory
WORKDIR /prink-job

# Copy the Maven project files
COPY pom.xml .
COPY src ./src

# Build the Maven project
RUN mvn clean package

# Stage 2: Create the final image
FROM flink:1.19

# Set the working directory
WORKDIR /prink-job

# Copy the JAR file from the builder stage
COPY --from=builder /prink-job/target/prink-job-1.0-SNAPSHOT.jar /prink-job/prink-job.jar

# Set the entrypoint to run the Flink job
ENTRYPOINT ["flink", "run", "-m", "flink-jobmanager:8081", "/prink-job/prink-job.jar"]
FROM maven as build
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Build the application using Maven
RUN mvn clean package -DskipTests

FROM flink

# Copy the JAR file from the build stage
COPY --from=build /app/target /opt/flink/usrlib/artifacts/1/

# Set an environment variable to the JAR file
RUN export JAR_FILE=$(ls /opt/flink/usrlib/artifacts/1/prink-*.jar) && \
    echo "JAR_FILE=$JAR_FILE" > /opt/flink/usrlib/artifacts/1/jar_env.sh

# Set the job manager address
ENV JOB_MANAGER_RPC_ADDRESS=flink-jobmanager

# Source the environment variable and run the Flink job
ENTRYPOINT ["/bin/bash", "-c", "source /opt/flink/usrlib/artifacts/1/jar_env.sh && flink run $JAR_FILE --job-classname ganges.GangesEvaluation"]
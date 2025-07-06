FROM maven as build
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Build the application using Maven
RUN mvn clean package -DskipTests

FROM flink:1.19

COPY --from=build /app/target/prink-*.jar /opt/flink/usrlib/artifacts/1/

# CMD [ "--job-classname", "ganges.GangesEvaluation" ]
ENTRYPOINT ["flink", "run", "-m", "flink-jobmanager:8081", "/opt/flink/usrlib/artifacts/1/prink-0.6.jar"]
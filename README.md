# anotherdemoapplication

![](else/architecture.png)

This is a demo application for a Master's thesis project.
### Fundamental ideas:
- k8s deployment
- microservices
- streaming data
- in transit anonymization

### Roadmap:
- [x] Set up Repo
- [x] load generator service
- [x] Kafka topic A
- [x] consumer service
- [x] Flink/Prink job
- [ ] graphana dashboard
- [ ] HDFS long term storage

### How to run:

Build the docker images:
```bash
docker-compose build
```

Run the services:
```bash
docker-compose up
```

Access the services:
- Kafka: Use kafka:9092 within Docker containers.
- Grafana: http://localhost:3000
- Flink Web UI: http://localhost:8081
- HDFS Namenode UI: http://localhost:9870

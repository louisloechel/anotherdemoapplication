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
- [x] Kafka topic
- [x] consumer service
- [ ] Flink/Prink job
- [x] graphana dashboard
- [ ] HDFS long term storage

### How to run:

Build & run the docker images:
```bash
docker-compose up --build
```

Grafana Dashboard:
- Grafana: http://localhost:3000
- Query: ```kafka_consumer_messages_total```
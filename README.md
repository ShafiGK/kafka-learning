# Kafka Learning & Practice

Hands-on Apache Kafka practice covering deployment, client development, 
and operations using Docker and Kubernetes.

##  Skills Demonstrated

- *Kafka Architecture*: Topics, partitions, consumer groups, replication
- *Deployment*: Docker Compose, Kubernetes (Strimzi Operator)
- *Client Development*: Python producers/consumers
- *Operations*: Broker failure recovery, consumer lag debugging
- *Monitoring*: Prometheus + Grafana integration

##  Contents

- docker-compose.yml - Single-node Kafka cluster with Zookeeper and Kafka UI
- producer.py - Python Kafka producer
- consumer.py - Python Kafka consumer

##  Quick Start

\\\`bash
# Start Kafka cluster
docker compose up -d

# Access Kafka UI
# http://localhost:8080

# Run producer
python3 producer.py

# Run consumer
python3 consumer.py
\\\`

##  Tech Stack

- Apache Kafka (Confluent Platform 7.5.0)
- Docker / Docker Compose
- Python (kafka-python)

##  Learning Journey

This repository documents my hands-on journey mastering Apache Kafka,
building from fundamentals to production-grade patterns.

##  Author

*Shafique Khan* 
Application Support Lead | DevOps & Platform Engineering 
shafigk1511@gmail.com

##  License

Educational purposes...

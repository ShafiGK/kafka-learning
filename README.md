# Apache Kafka Learning Journey

> Hands-on exploration of Apache Kafka — from fundamentals
> to production patterns. Built using Docker, Kubernetes,
> and Python.

## 🎯 What I'm Learning

- **Architecture:** Topics, partitions, consumer groups, replication
- **Deployment:** Docker Compose, Kubernetes (Strimzi)
- **Clients:** Python producers and consumers
- **Operations:** Broker failure recovery, lag debugging
- **Monitoring:** Prometheus + Grafana integration

## 🚀 Quick Start

### Prerequisites

- Linux/macOS
- Docker & Docker Compose
- Python 3.8+

### Run Locally

```bash
# Start Kafka cluster
docker compose up -d

# Access Kafka UI
open http://localhost:8080

# Run producer
python3 producer.py

# Run consumer
python3 consumer.py
```

## 📂 Repository Structure

```
kafka-learning/
├── docker-compose.yml    # Single-node Kafka setup
├── producer.py           # Python Kafka producer
├── consumer.py           # Python Kafka consumer
└── README.md             # This file
```

## 🔧 Tech Stack

| Component | Version |
|-----------|---------|
| Apache Kafka | 7.5.0 |
| Zookeeper | 7.5.0 |
| Kafka UI | latest |
| Python | 3.8+ |

## 📚 Learning Path

- [x] Basic Kafka deployment with Docker
- [x] Python producer/consumer
- [ ] Multi-broker cluster
- [ ] Kubernetes deployment with Strimzi
- [ ] Production monitoring setup
- [ ] Schema Registry integration
- [ ] Kafka Connect

## 👤 Author

**Shafique Khan**
Application Support Lead | Platform Engineering
📧 shafigk1511@gmail.com
💼 [LinkedIn](https://linkedin.com/in/shafique-khan-a835551b5)

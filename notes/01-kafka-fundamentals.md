# Kafka Fundamentals

## Core Concepts

### Topics
- Logical channels for messages
- Producers write, consumers read
- Identified by name (e.g., 'orders', 'user-events')

### Partitions
- Topics are split into partitions for parallelism
- Each partition is ordered, immutable sequence
- Messages have offsets within a partition

### Producers
- Send messages to topics
- Choose partition via key (hash) or round-robin
- Configure acks: 0 (none), 1 (leader), all (ISR)

### Consumers & Consumer Groups
- Consumers read from partitions
- Consumer groups share work across partitions
- Each partition consumed by one consumer in a group

### Brokers
- Kafka servers that store data
- Cluster has multiple brokers
- Each partition has a leader broker

### Replication
- Partitions replicated across brokers
- Replication factor determines copies
- ISR = In-Sync Replicas

## Key Commands

```bash
# Create topic
kafka-topics --create --topic my-topic --partitions 3 \
  --replication-factor 1 --bootstrap-server localhost:9092

# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics --describe --topic my-topic \
  --bootstrap-server localhost:9092
```

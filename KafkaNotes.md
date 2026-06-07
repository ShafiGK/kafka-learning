# 1 Go to directoryfolder
cd ~/kafka-learning
python3 -m venv venv
source venv/bin/activate

# 2 Bring up kafka, zookeeper and UI
docker compose up -d 
# Validate
docker ps

# 3 Enter kafka container
3.1 Enter Kafka Container
docker exec -it kafka bash
3.2 Create a Topic
kafka-topics --create --topic learn-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
3.3 List Topics
kafka-topics --list --bootstrap-server localhost:9092
3.4 Describe Topic
kafka-topics --describe --topic learn-topic --bootstrap-server localhost:9092
3.5 Producer (Send Messages)
kafka-console-producer --topic learn-topic --bootstrap-server localhost:9092
Type messages and press Enter. Each line is a message. Ctrl+C to exit.
3.6 Consumer (Read Messages)
Open a NEW terminal and enter the container again:
docker exec -it kafka bash
kafka-console-consumer --topic learn-topic --bootstrap-server localhost:9092 --from-beginning
3.7 Consumer with Group
kafka-console-consumer --topic learn-topic \
  --bootstrap-server localhost:9092 \
  --group learn-group-1 \
  --from-beginning
3.8 Check Consumer Groups
kafka-consumer-groups --list --bootstrap-server localhost:9092
kafka-consumer-groups --describe --group learn-group-1 --bootstrap-server localhost:9092




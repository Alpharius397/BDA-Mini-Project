## Start Server
```bash
    kafka-storage.sh format --standalone -t $(kafka-storage.sh random-uuid) -c config/server.properties
    kafka-server-start.sh config/server.properties
```

## Create Topic
```bash
    kafka-topics.sh --bootstrap-server localhost:9092 --create --topic KafkaMusicStream
```
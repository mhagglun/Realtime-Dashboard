# Usage

Start Zookeeper
```bash
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```

Start kafka
```bash
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Create topic
```bash
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic access-log
```


Execute the following script to start zookeeper and kafka, create a topic and start the producer.
```bash
./start-producer
```

The messages can be viewed through
```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic avg --from-beginning
```
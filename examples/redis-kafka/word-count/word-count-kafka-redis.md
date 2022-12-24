# Word count example using Kafka + Redis

## Install

1. [Redis cli](https://redis.io/docs/getting-started/)
2. [Kafka cli](https://kafka.apache.org/quickstart)

## Run

1. Start Kafka

```shell
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
```

2. Init Kafka topics

```shell
bin/kafka-topics.sh --create --topic sentences --bootstrap-server localhost:9092 &&
bin/kafka-topics.sh --create --topic sentences-word-count --bootstrap-server localhost:9092 &&
bin/kafka-topics.sh --create --topic word-count --bootstrap-server localhost:9092
```

3. Start Redis

```shell
docker run redis
```
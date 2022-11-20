# Kafka in Action

This project is a playground for Kafka apps written in Rust.

## Development

The following environments ar provided:

- single zookeeper / single broker:

  ```bash
  # Start
  docker-compose -f docker/zk-single-kafka-single.yml up -d
  # Stop
  docker-compose -f docker/zk-single-kafka-single.yml down
  ```
- single zookeeper / multiple broker:

  ```bash
  # Start
  docker-compose -f docker/zk-single-kafka-multiple.yml up -d
  # Stop
  docker-compose -f docker/zk-single-kafka-multiple.yml down
  ```

Create a topic:

```bash
docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --topic topic1 --describe
```

Describe a topic:

```bash
docker exec -it kafka1 kafka-topics --bootstrap-server localhost:9092 --topic topic1 --create --replication-factor 1 --partitions 3
```

## TODO

- [ ] Serialize and deserialize from Confluence Schema Registry using [schema_registry_converter](https://github.com/gklijs/schema_registry_converter)

## Credit

- docker-compose: https://github.com/conduktor/kafka-stack-docker-compose

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

## Credit

- docker-compose: https://github.com/conduktor/kafka-stack-docker-compose

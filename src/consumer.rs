use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig,
};

use crate::config::*;

pub fn create_consumer(brokers: Vec<Broker>, topic: TopicName, group_id: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers_to_str(brokers))
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&topic.0])
        .expect("Can't subscribe to specified topic");

    consumer
}

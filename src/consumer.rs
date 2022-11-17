use crate::config::*;
use rdkafka::{
    config::FromClientConfig,
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    ClientConfig,
};

fn create_consumer<T>(brokers: Vec<Broker>, topics: &[&str], group_id: &str) -> T
where
    T: FromClientConfig,
    T: Consumer,
{
    let consumer: T = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers_to_str(brokers))
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topic");

    consumer
}

pub fn create_base_consumer(brokers: Vec<Broker>, topics: &[&str], group_id: &str) -> BaseConsumer {
    create_consumer(brokers, topics, group_id)
}

pub fn create_stream_consumer(
    brokers: Vec<Broker>,
    topics: &[&str],
    group_id: &str,
) -> StreamConsumer {
    create_consumer(brokers, topics, group_id)
}

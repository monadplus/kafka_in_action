use rdkafka::{producer::FutureProducer, ClientConfig};

use crate::config::*;

pub fn create_producer(brokers: Vec<Broker>) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers_to_str(brokers))
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed")
}

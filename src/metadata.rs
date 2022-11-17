use crate::config::*;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    ClientConfig,
};
use std::time::Duration;
use tracing::{error, info};

pub fn create_admin(brokers: Vec<Broker>) -> AdminClient<rdkafka::client::DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers_to_str(brokers))
        .create()
        .expect("Producer creation error")
}

#[tracing::instrument(skip_all)]
async fn describe_topic(brokers: Vec<Broker>, topic: TopicName) {
    let admin = create_admin(brokers);
    let opts = AdminOptions::new();
    let resources = [&ResourceSpecifier::Topic(&topic.0)];
    for r in admin.describe_configs(resources, &opts).await.unwrap() {
        let r = r.unwrap();
        info!("Resource: {:?}", r.specifier);
        for (k, v) in r.entry_map() {
            info!("\t{}: {:?}", k, v.value);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn create_topic(brokers: Vec<Broker>, topic: TopicName) {
    let admin = create_admin(brokers);
    let new_topic = NewTopic::new(topic.0.as_ref(), 5, TopicReplication::Fixed(1));
    let opts = AdminOptions::new();
    for result in admin.create_topics([&new_topic], &opts).await.unwrap() {
        match result {
            Ok(name) => info!(r#"Topic "{name}" created."#),
            Err((name, reason)) => error!(r#"Topic "{}" couldn't be created: {:?}"#, name, reason),
        }
    }
}

pub fn log_metadata(brokers: &str, topic: Option<&str>, timeout: Duration, fetch_offsets: bool) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(topic, timeout)
        .expect("Failed to fetch metadata");

    let mut message_count = 0;

    info!("Cluster information:");
    info!("  Broker count: {}", metadata.brokers().len());
    info!("  Topics count: {}", metadata.topics().len());
    info!("  Metadata broker name: {}", metadata.orig_broker_name());
    info!("  Metadata broker id: {}\n", metadata.orig_broker_id());

    info!("Brokers:");
    for broker in metadata.brokers() {
        info!(
            "  Id: {}  Host: {}:{}  ",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }

    info!("\nTopics:");
    for topic in metadata.topics() {
        info!("  Topic: {}  Err: {:?}", topic.name(), topic.error());
        for partition in topic.partitions() {
            info!(
                "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                partition.id(),
                partition.leader(),
                partition.replicas(),
                partition.isr(),
                partition.error()
            );
            if fetch_offsets {
                let (low, high) = consumer
                    .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                    .unwrap_or((-1, -1));
                info!(
                    "       Low watermark: {}  High watermark: {} (difference: {})",
                    low,
                    high,
                    high - low
                );
                message_count += high - low;
            }
        }
        if fetch_offsets {
            info!("     Total message count: {}", message_count);
        }
    }
}

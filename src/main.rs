//! How to run:
//!
//! ```
//! $ cargo run -- --broker=localhost:9092 --broker=localhost:9093 --topic=example
//! ```

use clap::Parser;
use itertools::Itertools;
use std::{str::FromStr, sync::Arc, time::Duration};
use thiserror::Error;

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication},
    consumer::{Consumer, StreamConsumer},
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::get_rdkafka_version,
    ClientConfig,
};
use tracing::{error, info, Level};

#[derive(Debug, Error)]
enum AppError {
    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Broker <host>:<port>
    #[arg(short, long = "broker", default_value = "localhost:9092")]
    brokers: Vec<Broker>,
    /// Destination topic
    #[arg(short, long, default_value = "example")]
    topic: TopicName,
}

#[derive(Debug, Clone, derive_more::Display)]
#[display(fmt = "{host}:{port}")]
struct Broker {
    host: String,
    port: u16,
}

impl FromStr for Broker {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.split_once(':')
            .and_then(|(host, port)| {
                let host = host.to_string();
                port.parse::<u16>().map(|port| Broker { host, port }).ok()
            })
            .ok_or_else(|| {
                AppError::InvalidArguments(
                    r#"Expected "<host>:<port>(,<host>:<port>)*""#.to_string(),
                )
            })
    }
}

fn brokers_to_str(brokers: impl IntoIterator<Item = Broker>) -> String {
    brokers
        .into_iter()
        .map(|broker| broker.to_string())
        .join(",")
}

#[derive(Debug, Clone, derive_more::Display, derive_more::FromStr)]
struct TopicName(String);

#[allow(dead_code)]
fn create_admin(brokers: Vec<Broker>) -> AdminClient<rdkafka::client::DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers_to_str(brokers))
        .create()
        .expect("Producer creation error")
}

#[allow(dead_code)]
fn create_consumer(brokers: Vec<Broker>, topic: TopicName, group_id: &str) -> StreamConsumer {
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

fn create_producer(brokers: Vec<Broker>) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers_to_str(brokers))
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed")
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

#[tracing::instrument]
async fn producer(brokers: Vec<Broker>, topic: TopicName) {
    let topic = Arc::new(topic);

    let producer: Arc<FutureProducer> = Arc::new(create_producer(brokers));

    let futures = (0..5)
        .map(|i| {
            let producer = Arc::clone(&producer);
            let topic = Arc::clone(&topic);
            async move {
                let delivery_status = producer
                    .send(
                        FutureRecord::to(&topic.to_string())
                            .payload(&format!("Message {}", i))
                            .key(&format!("Key {}", i))
                            .headers(OwnedHeaders::new().insert(Header {
                                key: "header_key",
                                value: Some("header_value"),
                            })),
                        Duration::from_secs(0),
                    )
                    .await;
                info!("Delivery status for message {} received", i);
                delivery_status
            }
        })
        .collect::<Vec<_>>();

    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .init();

    let (_, version_s) = get_rdkafka_version();
    info!("rd_kafka_verison {version_s}");

    // create_topic(args.brokers, args.topic).await;
    // describe_topic(args.brokers, args.topic).await;

    producer(args.brokers, args.topic).await;
}

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
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::get_rdkafka_version,
    ClientConfig,
};
use tracing::{info, Level};

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

#[derive(Debug, Clone, derive_more::Display, derive_more::FromStr)]
struct TopicName(String);

#[tracing::instrument]
async fn producer(brokers: Vec<Broker>, topic: TopicName) {
    let topic = Arc::new(topic);

    let producer: FutureProducer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            &brokers
                .into_iter()
                .map(|broker| broker.to_string())
                .join(",")[..],
        )
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    let producer = Arc::new(producer);

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
        .compact()
        .init();

    let (_, version_s) = get_rdkafka_version();
    println!("rd_kafka_verison {version_s}");

    producer(args.brokers, args.topic).await;
}

//! How to run:
//!
//! ```
//! $ cargo run -- --broker=localhost:9092 --broker=localhost:9093 --topic=example
//! ```

use clap::Parser;
use kafka_in_action::config::*;
use kafka_in_action::producer::*;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::get_rdkafka_version,
};
use std::{sync::Arc, time::Duration};
use tracing::{info, Level};

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

#[tracing::instrument]
async fn run(brokers: Vec<Broker>, topic: TopicName) {
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

    run(args.brokers, args.topic).await;
}

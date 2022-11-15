use std::time::Duration;

use clap::Parser;

use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::get_rdkafka_version,
    ClientConfig,
};
use tracing::{info, Level};

// TODO: newtype for brokers and topic
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Broker list
    #[arg(short, long, default_value = "localhost:9092")]
    brokers: Vec<String>,
    /// Destination topic
    #[arg(short, long, default_value = "example")]
    topic: String,
}

#[tracing::instrument]
async fn producer(brokers: Vec<String>, topic: String) {
    let topic_ref = topic.as_ref();
    let brokers_ref = &brokers.join(",")[..];

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers_ref)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let futures = (0..5)
        .map(|i| async move {
            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_ref)
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
        })
        .collect::<Vec<_>>();

    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}

// $ cargo run -- --brokers=localhost:9092,localhost:9093 --topic=example
#[tokio::main]
async fn main() {
    let Args { brokers, topic } = Args::parse();

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .compact()
        .init();

    let (_, version_s) = get_rdkafka_version();
    println!("rd_kafka_verison {version_s}");

    producer(brokers, topic).await;
}

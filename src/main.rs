//! How to run:
//!
//! ```
//! $ cargo run -- --broker=localhost:9092 --broker=localhost:9093 --topic=example
//! ```

use clap::Parser;
use futures::future::join_all;
use futures::future::try_join_all;
use kafka_in_action::config::*;
use kafka_in_action::consumer::*;
use kafka_in_action::metadata::*;
use kafka_in_action::producer::*;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::util::Timeout;
use rdkafka::Message;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::get_rdkafka_version,
};
use std::{sync::Arc, time::Duration};
use tokio::task::yield_now;
use tracing::warn;
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

#[tracing::instrument(skip_all)]
async fn run_producer(
    i: i32,
    topic: TopicName,
    producer: Arc<FutureProducer>,
) -> OwnedDeliveryResult {
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
    delivery_status
}

#[tracing::instrument(skip(consumer))]
async fn run_consumer(i: usize, consumer: BaseConsumer) {
    loop {
        match consumer.poll(Timeout::After(Duration::from_secs(5))) {
            None => {
                break;
            }
            Some(Err(e)) => {
                warn!("poll error: {}", e);
            }
            Some(Ok(m)) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                info!(
                    key = ?m.key(),
                    %payload,
                    topic = %m.topic(),
                    partition = %m.partition(),
                    offset = %m.offset(),
                    timestamp = ?m.timestamp(),
                    "Consumer message received",
                );
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
        yield_now().await;
    }
}

#[tracing::instrument]
async fn run(brokers: Vec<Broker>, topic: TopicName) {
    let producer: Arc<FutureProducer> = Arc::new(create_producer(brokers.clone()));
    let producer_handlers = (0..10)
        .map(|i| {
            let producer = Arc::clone(&producer);
            run_producer(i, topic.clone(), producer)
        })
        .collect::<Vec<_>>();

    let consumer_handlers = (0..3)
        .map(|i| {
            let consumer: BaseConsumer = create_base_consumer(brokers.clone(), &[&topic], "group1");
            tokio::spawn(run_consumer(i, consumer))
        })
        .collect::<Vec<_>>();

    match try_join_all(producer_handlers).await {
        Err(e) => warn!("Futures error: {:?}", e),
        Ok(results) => {
            results.into_iter().for_each(|(partition, offset)| {
                info!(%partition, %offset, "Producer message successfully sent");
            });
        }
    }

    join_all(consumer_handlers).await;
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
    info!("rd_kafka_verison {version_s}\n");

    // create_topic(args.brokers, args.topic, 10).await;
    // describe_topic(args.brokers, args.topic).await;
    // get_metadata(args.brokers.clone(), Some(args.topic.clone()), true);

    run(args.brokers, args.topic).await;
}

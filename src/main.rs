//! How to run:
//!
//! ```
//! $ cargo run -- --broker=localhost:9092 --broker=localhost:9093 --topic=example
//! ```

use clap::Parser;
use futures::future::join_all;
use futures::future::try_join_all;
use futures::StreamExt;
use kafka_in_action::config::*;
use kafka_in_action::consumer::*;
use kafka_in_action::metadata::*;
use kafka_in_action::producer::*;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::util::Timeout;
use rdkafka::Message;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::get_rdkafka_version,
};
use serde::Deserialize;
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::task::yield_now;
use tokio::time::timeout;
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Action {
    Deposit {
        from: String,
        to: String,
        amount: u64,
    },
    Withdraw {
        from: String,
        to: String,
        amount: u64,
    },
}

lazy_static::lazy_static! {
    static ref ACTIONS: [Action; 2] = [
        Action::Deposit {
            from: "A".to_string(),
            to: "B".to_string(),
            amount: 15 as u64,
        },
        Action::Withdraw {
            from: "A".to_string(),
            to: "C".to_string(),
            amount: 10 as u64,
        },
    ];
}

#[tracing::instrument(skip_all)]
async fn run_producer(
    i: i32,
    topic: TopicName,
    producer: Arc<FutureProducer>,
) -> OwnedDeliveryResult {
    let payload: String = serde_json::to_string(&ACTIONS[i as usize % 2]).unwrap();
    let delivery_status = producer
        .send(
            FutureRecord::to(&topic.to_string())
                .payload(&payload)
                .key(&format!("Key {}", i)),
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
                let payload: Action = match m.payload_view::<str>() {
                    Some(Ok(str)) => match serde_json::from_str::<Action>(str) {
                        Ok(payload) => payload,
                        Err(e) => {
                            warn!("Error while deserializing json payload: {:?}", e);
                            continue;
                        }
                    },
                    None => continue,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        continue;
                    }
                };
                info!(
                    key = ?m.key(),
                    ?payload,
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

#[tracing::instrument(skip(consumer))]
async fn run_stream_consumer(i: usize, consumer: StreamConsumer) {
    let consumer = Arc::new(consumer);
    let stream = consumer.stream().for_each_concurrent(10, |m| {
        let consumer = Arc::clone(&consumer);
        async move {
            match m {
                Err(e) => {
                    warn!("poll error: {}", e);
                }
                Ok(m) => {
                    match m.payload_view::<str>() {
                        Some(Ok(str)) => match serde_json::from_str::<Action>(str) {
                            Ok(payload) => {
                                info!(
                                    key = ?m.key(),
                                    ?payload,
                                    topic = %m.topic(),
                                    partition = %m.partition(),
                                    offset = %m.offset(),
                                    timestamp = ?m.timestamp(),
                                    "Consumer message received",
                                );
                            }
                            Err(e) => {
                                warn!("Error while deserializing json payload: {:?}", e);
                            }
                        },
                        None => {}
                        Some(Err(e)) => {
                            warn!("Error while deserializing message payload: {:?}", e);
                        }
                    };
                    consumer.commit_message(&m, CommitMode::Async).unwrap();
                }
            };
        }
    });

    if let Err(_) = timeout(Duration::from_secs(5), stream).await {
        info!("Stream {} timed out (as expected)", i);
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

    // let consumer_handlers = (0..3)
    //     .map(|i| {
    //         let consumer: BaseConsumer = create_base_consumer(brokers.clone(), &[&topic], "group1");
    //         tokio::spawn(run_consumer(i, consumer))
    //     })
    //     .collect::<Vec<_>>();

    let consumer_handlers = (0..3)
        .map(|i| {
            let consumer: StreamConsumer =
                create_stream_consumer(brokers.clone(), &[&topic], "group1");
            tokio::spawn(run_stream_consumer(i, consumer))
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
    get_metadata(args.brokers.clone(), Some(args.topic.clone()), true);

    run(args.brokers, args.topic).await;
}

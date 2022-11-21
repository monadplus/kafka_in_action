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
use schema_registry_converter::async_impl::easy_json::EasyJsonDecoder;
use schema_registry_converter::async_impl::easy_json::EasyJsonEncoder;
use schema_registry_converter::async_impl::json::DecodeResult;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
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
    encoder: Arc<EasyJsonEncoder>,
) -> OwnedDeliveryResult {
    let value = serde_json::to_value(&ACTIONS[i as usize % 2]).unwrap();
    let strategy = SubjectNameStrategy::TopicNameStrategy(topic.clone().0, false);
    let payload = encoder.encode(&value, strategy).await.unwrap();
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

#[tracing::instrument(skip(consumer, decoder))]
async fn run_stream_consumer(i: usize, consumer: StreamConsumer, decoder: Arc<EasyJsonDecoder>) {
    let consumer = Arc::new(consumer);
    let stream = consumer.stream().for_each_concurrent(10, |m| {
        let consumer = Arc::clone(&consumer);
        let decoder = Arc::clone(&decoder);
        async move {
            match m {
                Err(e) => {
                    warn!("poll error: {}", e);
                }
                Ok(m) => {
                    match m.payload_view::<[u8]>() {
                        Some(Ok(bytes)) => match decoder.decode(Some(bytes)).await {
                            Ok(result) => {
                                let DecodeResult { schema, value } = result.unwrap();
                                schema_registry_converter::async_impl::json::validate(
                                    schema, &value,
                                )
                                .unwrap();
                                let payload = serde_json::from_value::<Action>(value).unwrap();
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

#[tracing::instrument(skip(encoder, decoder))]
async fn run(
    brokers: Vec<Broker>,
    topic: TopicName,
    encoder: Arc<EasyJsonEncoder>,
    decoder: Arc<EasyJsonDecoder>,
) {
    let producer: Arc<FutureProducer> = Arc::new(create_producer(brokers.clone()));
    let producer_handlers = (0..10)
        .map(|i| {
            let producer = Arc::clone(&producer);
            let encoder = Arc::clone(&encoder);
            run_producer(i, topic.clone(), producer, encoder)
        })
        .collect::<Vec<_>>();

    // let consumer_handlers = (0..3)
    //     .map(|i| {
    //         let consumer: StreamConsumer =
    //             create_stream_consumer(brokers.clone(), &[&topic], "group1");
    //         let decoder = Arc::clone(&decoder);
    //         tokio::spawn(run_stream_consumer(i, consumer, decoder))
    //     })
    //     .collect::<Vec<_>>();

    match try_join_all(producer_handlers).await {
        Err(e) => warn!("Futures error: {:?}", e),
        Ok(results) => {
            results.into_iter().for_each(|(partition, offset)| {
                info!(%partition, %offset, "Producer message successfully sent");
            });
        }
    }

    // join_all(consumer_handlers).await;
}

/* FIXME:I guess the schema does not exist in the registry

thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: SRCError { error: "Could not get id from response", cause: None, retriable: false, cached: true }', src/main.rs:88:58
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


kafka-schema-registry  | [2022-11-21 07:58:17,114] ERROR Request Failed with exception  (io.confluent.rest.exceptions.DebuggableExceptionMapper)
kafka-schema-registry  | io.confluent.rest.exceptions.RestNotFoundException: Subject 'topic1-value' not found.
kafka-schema-registry  |        at io.confluent.kafka.schemaregistry.rest.exceptions.Errors.subjectNotFoundException(Errors.java:77)
kafka-schema-registry  |        at io.confluent.kafka.schemaregistry.rest.resources.SubjectVersionsResource.getSchemaByVersion(SubjectVersionsResource.java:150)

...

kafka-schema-registry  | [2022-11-21 07:58:17,128] INFO 172.21.0.1 - - [21/Nov/2022:07:58:17 +0000] "GET /subjects/topic1-value/versions/latest HTTP/1.1" 404 66 "-" "-" 68 (io.confluent.rest-utils.requests)
*/

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

    let encoder = Arc::new(EasyJsonEncoder::new(SrSettings::new(String::from(
        "http://localhost:8081",
    ))));
    let decoder = Arc::new(EasyJsonDecoder::new(SrSettings::new(String::from(
        "http://localhost:8081",
    ))));

    // create_topic(args.brokers, args.topic, 10).await;
    // describe_topic(args.brokers, args.topic).await;
    get_metadata(args.brokers.clone(), Some(args.topic.clone()), true);

    run(args.brokers, args.topic, encoder, decoder).await;
}

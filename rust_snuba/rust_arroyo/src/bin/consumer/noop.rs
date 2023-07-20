extern crate rust_arroyo;

use clap::{App, Arg};
use log::info;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::processing::strategies::ProcessingStrategy;
use rust_arroyo::processing::strategies::{commit_offsets, ProcessingStrategyFactory};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Partition, Topic};
use std::collections::HashMap;
use std::time::Duration;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

struct StrategyFactory {
    batch_time: u64,
}
impl ProcessingStrategyFactory<KafkaPayload> for StrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(commit_offsets::new(Duration::from_millis(self.batch_time)))
    }
}

fn main() {
    let matches = App::new("noop consumer")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple noop consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("source-topic")
                .long("source-topic")
                .help("source topic name")
                .default_value("test_source")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch-time")
                .long("batch-time")
                .help("time of the batch for flushing")
                .default_value("100")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("offset-reset")
                .long("offset-reset")
                .help("kafka auto.offset.reset param")
                .default_value("earliest")
                .takes_value(true),
        )
        .get_matches();

    env_logger::init();
    let source_topic = matches.value_of("source-topic").unwrap();
    let offset_reset = matches.value_of("offset-reset").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let batch_time = matches
        .value_of("batch-time")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let config = KafkaConfig::new_consumer_config(
        vec![brokers.to_string()],
        group_id.to_string(),
        offset_reset.to_string(),
        false,
        None,
    );
    let consumer = KafkaConsumer::new(config);
    let topic = Topic {
        name: source_topic.to_string(),
    };
    let mut stream_processor =
        StreamProcessor::new(Box::new(consumer), Box::new(StrategyFactory { batch_time }));

    info!("Starting no-op consumer");
    stream_processor.subscribe(topic);
    stream_processor.run().unwrap();
}
